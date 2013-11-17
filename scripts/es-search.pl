#!/usr/bin/env perl
# PODNAME: es-search.pl
# ABSTRACT: Provides a CLI for quick searches of data in ElasticSearch daily indexes
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use Carp;
use DateTime;
use Getopt::Long;
use JSON::XS;
use MIME::Lite;
use Pod::Usage;
use YAML;

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT,
    'asc',
    'desc',
    'exists:s',
    'missing:s',
    'size|n:i',
    'show:s',
    'fields',
    'help|h',
    'manual|m',
);

# Search string is the rest of the argument string
my $search_string = join(' ', expand_ip_to_range(@ARGV));

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitval => 0, -verbose => 2) if $OPT{manual};

#--------------------------------------------------------------------------#
# App Config
my %CONFIG = (
    size         => (exists $OPT{size} && $OPT{size} > 0 ? int($OPT{size}) : 20),
);

#------------------------------------------------------------------------#
# Connect to ElasticSearch
my $ES = es_connect();

# Handle Indices
my $ORDER = exists $OPT{asc} && $OPT{asc} ? 'asc' : 'desc';
my %indices = map { $_ => es_index_days_old($_) } es_indices();

# Which fields to show
my @SHOW = ();
if ( exists $OPT{show} && length $OPT{show} ) {
    @SHOW = split /,/, $OPT{show};
}

if( $OPT{fields} ) {
    show_fields();
    exit 0;
}
pod2usage({-exitval => 1, -msg => 'No search string specified'}) unless defined $search_string and length $search_string;

# Fix common mistakes
$search_string =~ s/\s+and\s+/ AND /g;
$search_string =~ s/\s+or\s+/ OR /g;

# Process extra parameters
my %extra = ();
if( exists $OPT{exists} ) {
    $extra{filter} = { exists => { field => $OPT{exists} } };
}
if( exists $OPT{missing} ) {
    $extra{filter} = { missing => { field => $OPT{missing} } };
}

my $size = $CONFIG{size} > 50 ? 50 : $CONFIG{size};
my @displayed_indices = ();
my $TOTAL_HITS = 0;
my $duration = 0;
my $displayed = 0;
my $header=0;
foreach my $index ( sort by_index_age keys %indices ) {
    my $result;
    my $start=time();
    eval {
        $result = $ES->search(
            index   => $index,
            query   => { query_string => { query => $search_string } } ,
            size    => $size,
            sort   => [ { '@timestamp' => $ORDER } ],
            scroll  => '10s',
            timeout => '5s',
            %extra,
        );
    };
    if( my $error = $@ ) {
        croak "ElasticSearch Error -> $error";
    }
    $duration += time() - $start;
    push @displayed_indices, $index;
    $TOTAL_HITS += $result->{hits}{total};

    my @always = qw(@timestamp);
    $header++ == 0 && @SHOW && output({color=>'cyan'}, join("\t", @always,@SHOW));
    while( $result ) {
        my $hits = $result->{hits}{hits};
        last unless @{$hits};

        foreach my $hit (@{ $hits }) {
            my $record = {};
            if( @SHOW ) {
                foreach my $f (qw(@timestamp)) {
                    $record->{$f} = $hit->{_source}{$f};
                }
                foreach my $f (@SHOW) {
                    $record->{$f} = exists $hit->{_source}{$f} ? $hit->{_source}{$f}
                                  : exists $hit->{_source}{'@fields'}{$f} ? $hit->{_source}{'@fields'}{$f}
                                  : undef;
                }
            }
            else {
                $record = $hit->{_source};
            }
            my $output =  @SHOW ? join("\t", map { exists $record->{$_} && defined $record->{$_} ? $record->{$_} : '-' } @always,@SHOW)
                       : Dump $record;
            output($output);
            $displayed++;
            last if $displayed >= $CONFIG{size};
        }

        last if $displayed >= $CONFIG{size};
        $result = $ES->scroll(
            scroll_id => $result->{_scroll_id},
            scroll    => '10s',
        );
    }
    last if $displayed >= $CONFIG{size};
}
output({stderr=>1,color=>'yellow'},
    "# Search string: $search_string",
    "# Displaying $displayed of $TOTAL_HITS in $duration seconds.",
    sprintf("# Indexes (%d of %d) searched: %s\n", scalar(@displayed_indices), scalar(@indices), join(',', @displayed_indices)),
);

sub extract_fields {
    my $ref = shift;
    my @keys = @_;

    my @fields = ();
    foreach my $key ( keys %{$ref} ) {
        if( exists $ref->{$key}{properties} ) {
            push @fields, extract_fields( $ref->{$key}{properties}, @keys, $key );
        }
        else {
            my $field = join('.', @keys, $key);
            if( $field =~ /^\@fields\.(.*)/ ) {
                $field .= " alias is $1";
            }
            push @fields, $field;
        }
    }
    return sort @fields;
}

sub show_fields {
    my $index = shift @indices;
    my $result = undef;
    eval {
        $result = $ES->mapping(index => $index, type => 'syslog');
    };

    if(! defined $result) {
        die "unable to read mapping for: $index\n";
    }

    my $prop_root = $result->{syslog}{properties};
    my @keys = extract_fields($prop_root);

    print map { "$_\n" } @keys;
}
sub by_index_age {
    return exists $OPT{asc}
        ? $indices{$a} <=> $indices{$b}
        : $indices{$b} <=> $indices{$a};
}

sub expand_ip_to_range {
    for ( @_ ) {
        s/^([^:]+_ip):(\d+\.\d+)\.\*(?:\.\*)?$/$1:[$2.0.0 $2.255.255]/;
        s/^([^:]+_ip):(\d+\.\d+\.\d+)\.\*$/$1:[$2.0 $2.255]/;
    }
    @_;
}
__END__

=head1 NAME

es-search.pl - Search a logging cluster for information

=head1 SYNOPSIS

es-search.pl [search string]

Options:

    --help              print help
    --manual            print full manual
    --show              Comma separated list of fields to display, default is ALL, switches to tab output
    --exists            Field which must be present in the document
    --missing           Field which must not be present in the document
    --index             Search only this index by name!
    --size              Result size, default is 20
    --asc               Sort by ascending timestamp
    --desc              Sort by descending timestamp (Default)
    --fields            Display the field list for this index!

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=item B<show>

Comma separated list of fields to display in the dump of the data

    --show src_ip,crit,file,out_bytes

=item B<exists>

Filter results to those containing a valid, not null field

    --exists referer

Only show records with a referer field in the document.

=item B<missing>

Filter results to those not containing a valid, not null field

    --missing referer

Only show records without a referer field in the document.

=item B<fields>

Display a list of searchable fields

=item B<index>

Search only this index for data, may also be a comma separated list

=item B<days>

The number of days back to search, the default is 5

=item B<base>

Index base name, will be expanded using the days back parameter.  The default
is 'logstash' which will expand to 'logstash-YYYY.MM.DD'

=item B<size>

The number of results to show, default is 20.

=back

=head1 DESCRIPTION

This tool takes a search string parameter to search the cluster.  It is in the format of the Lucene
L<query string|http://lucene.apache.org/core/2_9_4/queryparsersyntax.html>

Examples might include:

    # Search for past 10 days vhost admin.example.com and client IP 1.2.3.4
    es-search.pl --days=10 --size=100 dst:"admin.example.com" AND src_ip:"1.2.3.4"

    # Search for all apache logs past 5 days with status 500
    es-search.pl program:"apache" AND crit:500

    # Search for all apache logs past 5 days with status 500 show only file and out_bytes
    es-search.pl program:"apache" AND crit:500 --show file,out_bytes

    # Search for ip subnet client IP 1.2.3.0 to 1.2.3.255 or 1.2.0.0 to 1.2.255.255
    es-search.pl --size=100 dst:"admin.example.com" AND src_ip:"1.2.3.*"
    es-search.pl --size=100 dst:"admin.example.com" AND src_ip:"1.2.*"

Helpful in building queries is the --fields options which lists the fields:


    es-search.pl --fields

