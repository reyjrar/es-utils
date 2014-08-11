#!/usr/bin/env perl
# PODNAME: es-search.pl
# ABSTRACT: Provides a CLI for quick searches of data in ElasticSearch daily indexes
$|=1;           # Flush STDOUT
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use Data::Dumper;
use Carp;
use CLI::Helpers qw(:all);
use File::Slurp qw(slurp);
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use Pod::Usage;
use POSIX qw(strftime);
use YAML;

# For Elements which are data structures
local $Data::Dumper::Indent = 0;
local $Data::Dumper::Terse = 1;
local $Data::Dumper::Sortkeys = 1;

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
    'top:s',
    'tail',
    'fields',
    'bases',
    'all',
    'no-header',
    'help|h',
    'manual|m',
);

# Search string is the rest of the argument string
my $search_string = join(' ', format_search_string(@ARGV));

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitval => 0, -verbose => 2) if $OPT{manual};
my $unknown_options = join ', ', grep /^--/, @ARGV;
pod2usage({-exitval => 1, -msg =>"Unknown option(s): $unknown_options"}) if $unknown_options;

#--------------------------------------------------------------------------#
# App Config
my %CONFIG = (
    size => (exists $OPT{size} && $OPT{size} > 0 ? int($OPT{size}) : 20),
);

#------------------------------------------------------------------------#
# Handle Indices
my $ORDER = exists $OPT{asc} && $OPT{asc} ? 'asc' : 'desc';
$ORDER = 'asc' if exists $OPT{tail};
my %by_age = ();
my %indices = map { $_ => es_index_days_old($_) } es_indices();
my %FIELDS = ();
foreach my $index (sort by_index_age keys %indices) {
    my $age = $indices{$index};
    $by_age{$age} ||= [];
    push @{ $by_age{$age} }, $index;
    @FIELDS{es_index_fields($index)} = ();
}
debug_var(\%by_age);
my @AGES = sort { $ORDER eq 'asc' ? $b <=> $a : $a <=> $b } keys %by_age;
debug({color=>"cyan"}, "Fields discovered.");
debug_var(\%FIELDS);

# Which fields to show
my @SHOW = ();
my %HASH_FIELDS = ();
if ( exists $OPT{show} && length $OPT{show} ) {
    @SHOW = grep { exists $FIELDS{$_} } split /,/, $OPT{show};
    # hash will contain '{fieldname.key1.key2.key3}' => { field => 'fieldname', key => [ 'key1', 'key2', 'key3' ] }
    %HASH_FIELDS = map { my @v = split( /\./, substr( $_, 1, -1 ) ); $_ => { field => shift( @v ), key => \@v  } } grep { /^\{.+\..+\}$/ } @SHOW;
    debug_var(\%HASH_FIELDS);
}
if( $OPT{bases} ) {
    show_bases();
    exit 0;
}
if( $OPT{fields} ) {
    show_fields();
    exit 0;
}
pod2usage({-exitval => 1, -msg => 'No search string specified'}) unless defined $search_string and length $search_string;
pod2usage({-exitval => 1, -msg => 'Cannot use --tail and --top together'}) if exists $OPT{tail} && $OPT{top};
pod2usage({-exitval => 1, -msg => 'Please specify --show with --tail'}) if exists $OPT{tail} && !@SHOW;

# Process extra parameters
my %extra = ();
my @filters = ();
if( exists $OPT{exists} ) {
    foreach my $field (split /[,:]/, $OPT{exists}) {
        push @filters, { exists => { field => $OPT{exists} } };
    }
}
if( exists $OPT{missing} ) {
    foreach my $field (split /[,:]/, $OPT{missing}) {
        push @filters, { missing => { field => $OPT{exists} } };
    }
}
if( @filters ) {
    $extra{filter} = @filters > 1 ? { and => \@filters } : shift @filters;
}

my $DONE = 1;
local $SIG{INT} = sub { $DONE=1 };

my $facet_header = '';
if( exists $OPT{top} ) {
    my @facet_fields = grep { length($_) && exists $FIELDS{$_} } map { s/^\s+//; s/\s+$//; lc } split ',', $OPT{top};
    croak(sprintf("Option --top takes up to two fields, found %d fields: %s\n", scalar(@facet_fields),join(',',@facet_fields)))
        unless @facet_fields > 0 && @facet_fields < 3;

    my @facet;
    if ( scalar @facet_fields == 1 ) {
        @facet = ( field => $facet_fields[0] );
        $facet_header = "count\t" . $facet_fields[0];
    } else {
        #generate a script as
        my $script_field = join " + ':' + ", map { "_doc['$_'].value" } @facet_fields;
        @facet = ( script_field => $script_field );
        $facet_header = "count\t" . join ':', @facet_fields;
    }
    $extra{facets} = { top => { terms => { @facet }, facet_filter => exists $extra{filter} ? $extra{filter} : {} } };

    if( exists $OPT{all} ) {
        verbose({color=>'cyan'}, "Facets with --all are limited to returning 1,000,000 results.");
        $extra{facets}->{top}{terms}{size} = 1_000_000;
    }
    else {
        $extra{facets}->{top}{terms}{size} = $CONFIG{size};
    }
    $CONFIG{size} = 0;  # and we do not want any results other than the facet data
}
elsif(exists $OPT{tail}) {
    $CONFIG{size} = 10;
    @AGES = ($AGES[-1]);
    $DONE = 0;
}

my $size = $CONFIG{size} > 50 ? 50 : $CONFIG{size};
my %displayed_indices = ();
my $TOTAL_HITS = 0;
my $last_hit_ts = undef;
my $duration = 0;
my $displayed = 0;
my $header = exists $OPT{'no-header'};
my $age = undef;
my %last_batch_id=();
my %FACET_TOTALS = ();

AGES: while( !$DONE || @AGES ) {
    $age = @AGES ? shift @AGES : $age;
    select(undef,undef,undef,1) if exists $OPT{tail} && $last_hit_ts;
    my $start=time();
    $last_hit_ts ||= strftime('%Y-%m-%dT%H:%M:%S%z',localtime($start));
    my $local_search_string = exists $OPT{tail} ? sprintf('%s AND @timestamp:[%s TO *]', $search_string, $last_hit_ts)
                                                : $search_string;
    debug({color=>'yellow'},"Search String is $local_search_string");
    output({color=>'yellow'}, "Faceting for on " . join(',', @{ $by_age{$age} })) if $OPT{top};
    my $result = es_request('_search',
        # Search Parameters
        {
            index     => $by_age{$age},
            uri_param => {
                timeout     => '10s',
                exists $OPT{top} ? () : (scroll => '30s'),
            },
            method => 'POST',
        },
        # Search Body
        {
            size       => $size,
            query      => { query_string => { query => $local_search_string } } ,
            sort       => [ { '@timestamp' => $ORDER } ],
            %extra,
        }
    );
    debug_var($result);
    $duration += time() - $start;
    next unless defined $result;

    $displayed_indices{$_} = 1 for @{ $by_age{$age} };
    $TOTAL_HITS += $result->{hits}{total};

    my @always = qw(@timestamp);
    $header++ == 0 && @SHOW && output({color=>'cyan'}, join("\t", @always,@SHOW));
    while( $result || !$DONE ) {
        my $hits = ref $result->{hits}{hits} eq 'ARRAY' ? $result->{hits}{hits} : [];

        # Handle Faceting
        my $facets = exists $result->{facets} ? $result->{facets}{top}{terms} : [];
        if( @$facets ) {
            output({color=>'cyan'},$facet_header);
            foreach my $facet ( @$facets ) {
                $FACET_TOTALS{$facet->{term}} ||= 0;
                $FACET_TOTALS{$facet->{term}} += $facet->{count};
                output("$facet->{count}\t$facet->{term}");
                $displayed++;
            }
            $TOTAL_HITS = $result->{facets}{top}{other} + $displayed;
            next AGES;
        }
        elsif(exists $result->{facets}{top}) {
            output({indent=>1,color=>'red'}, "= No results.");
            next AGES;
        }

        # Reset the last batch ID if we have new data
        %last_batch_id = () if @{$hits} > 0 && $last_hit_ts ne $hits->[-1]->{_source}{'@timestamp'};
        debug({color=>'magenta'}, "+ ID cache is now empty.") unless keys %last_batch_id;

        foreach my $hit (@{ $hits }) {
            # Skip if we've seen this record
            next if exists $last_batch_id{$hit->{_id}};

            $last_hit_ts = $hit->{_source}{'@timestamp'};
            $last_batch_id{$hit->{_id}}=1;
            my $record = {};
            if( @SHOW ) {
                foreach my $f (@always) {
                    $record->{$f} = $hit->{_source}{$f};
                }
                foreach my $f (@SHOW) {
                    $record->{$f} = exists $HASH_FIELDS{$f} ? extract_value( $HASH_FIELDS{$f}{key}, $hit->{_source}{$HASH_FIELDS{$f}{field}}, $hit->{_source}{'@fields'}{$HASH_FIELDS{$f}{field}} )
                                  : exists $hit->{_source}{$f} ? $hit->{_source}{$f}
                                  : exists $hit->{_source}{'@fields'}{$f} ? $hit->{_source}{'@fields'}{$f}
                                  : undef;
                }
            }
            else {
                $record = $hit->{_source};
            }
            # Determine how this record is output
            my $output = undef;
            if( @SHOW ) {
                my @cols=();
                foreach my $f (@always,@SHOW) {
                    my $v = '-';
                    if( exists $record->{$f} && defined $record->{$f} ) {
                        $v = ref $record->{$f} ? Dumper $record->{$f} : $record->{$f};
                    }
                    push @cols,$v;
                }
                $output = join("\t",@cols);
            }
            else {
                $output = Dump $record;
            }

            output({data=>1}, $output);
            $displayed++;
            last if !exists $OPT{all} && $DONE && $displayed >= $CONFIG{size};
        }
        last if !exists $OPT{all} && $DONE && $displayed >= $CONFIG{size};

        # Scroll forward
        $start = time;
        $result = es_request('_search/scroll', {
            uri_param => {
                scroll_id => $result->{_scroll_id},
                scroll    => '30s',
            }
        });
        $duration += time - $start;
        last unless @{ $result->{hits}{hits} } > 0;
    }
    last if !exists $OPT{all} && $DONE && $displayed >= $CONFIG{size};
}

output({stderr=>1,color=>'yellow'},
    "# Search string: $search_string",
    "# Displaying $displayed of $TOTAL_HITS in $duration seconds.",
    sprintf("# Indexes (%d of %d) searched: %s\n",
            scalar(keys %displayed_indices),
            scalar(keys %indices),
            join(',', sort keys %displayed_indices)
    ),
);

if(keys %FACET_TOTALS) {
    output({color=>'yellow'}, '#', '# Totals across batch', '#');
    output({color=>'cyan'},$facet_header);
    foreach my $k (sort { $FACET_TOTALS{$b} <=> $FACET_TOTALS{$a} } keys %FACET_TOTALS) {
        output({data=>1,color=>'green'},"$FACET_TOTALS{$k}\t$k");
    }
}

sub extract_value {
    my ($key, $v1, $v2) = @_;
    my $value = $v1 ? $v1 : $v2;
    $value = ref($value) eq 'HASH' ? $value->{$_} : ref($value) eq 'ARRAY' ? $value->[$_] : undef for @{ $key };
    return $value;
}

sub show_fields {
    output({color=>'cyan'}, 'Fields available for search:' );
    my $total = 0;
    foreach my $field (sort keys %FIELDS) {
        $total++;
        output(" - $field");
    }
    output({color=>"yellow"},
        sprintf("# Fields: %d from a combined %d indices.\n",
            $total,
            scalar(keys %indices),
        )
    );
}

sub show_bases {
    output({color=>'cyan'}, 'Bases available for search:' );
    my $bases = {};
    foreach my $index (sort keys \%indices) {
        foreach my $base (split('-', $index)) {
            $bases->{$base} = '';
        }
    }
    foreach my $base (sort keys $bases) {
        output(" - $base");
    }

    output({color=>"yellow"},
        sprintf("# Bases: %d from a combined %d indices.\n",
            scalar(keys $bases),
            scalar(keys %indices),
        )
    );
}

sub by_index_age {
    return $ORDER eq 'asc'
        ? $indices{$b} <=> $indices{$a}
        : $indices{$a} <=> $indices{$b};
}

my %BareWords;
sub format_search_string {
    my @modified = ();
    %BareWords = map { $_ => uc } qw(and not or);
    foreach my $part ( @_ ) {
        if( my ($term,$match) = split /\:/, $part, 2 ) {
            if( defined $match && $match =~ /\.dat$/ && -f $match ) {
                my @data = grep { defined && length } slurp($match);
                if( @data ) {
                    chomp(@data);
                    push @modified,"$term:(" . join(' OR ', @data) . ")";
                    next;
                }
            }
            else {
                $part =~ s/^([^:]+_ip):(\d+\.\d+)\.\*(?:\.\*)?$/$1:[$2.0.0 $2.255.255]/;
                $part =~ s/^([^:]+_ip):(\d+\.\d+\.\d+)\.\*$/$1:[$2.0 $2.255]/;
            }
        }
        push @modified, exists $BareWords{lc $part} ? $BareWords{lc $part} : $part;
    }
    @modified;
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
    --tail              Continue the query until CTRL+C is sent
    --top               Perform a facet on the fields, by a comma separated list of up to 2 items
    --exists            Field which must be present in the document
    --missing           Field which must not be present in the document
    --size              Result size, default is 20
    --all               Don't consider result size, just give me *everything*
    --asc               Sort by ascending timestamp
    --desc              Sort by descending timestamp (Default)
    --no-header         Do not show the header with field names in the query results
    --fields            Display the field list for this index!
    --bases             Display the index base list for this cluster.

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

=item B<tail>

Repeats the query every second until CTRL+C is hit, displaying new results.  Due to the implementation,
this mode enforces that only the most recent indices are searched.  Also, given the output is continuous, you must
specify --show with this option.

=item B<top>

Comma separated list of fields to facet on.  Given that this uses scripted facets for multi-field facets,
it is limited to faceting on up to 2 fields.  This option is not available when using --tail

    --top src_ip


=item B<exists>

Filter results to those containing a valid, not null field

    --exists referer

Only show records with a referer field in the document.

=item B<missing>

Filter results to those not containing a valid, not null field

    --missing referer

Only show records without a referer field in the document.

=item B<bases>

Display a list of bases that can be used with the --base option.

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

=item B<all>

If specified, ignore the --size parameter and show me everything within the date range I specified.
In the case of --top, this limits the result set to 1,000,000 results.

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

    # Show the top src_ip for 'www.example.com'
    es-search.pl --base access dst:www.example.com --top src_ip

    # Tail the access log for www.example.com 404's
    es-search.pl --base access --tail --show src_ip,file,referer_domain dst:www.example.com AND crit:404

=head2 Extended Syntax

The search string is pre-analyzed before being sent to ElasticSearch.  Basic formatting is corrected:

The following barewords are transformed:

    or => OR
    and => AND
    not => NOT

If a field is an IP address wild card, it is transformed:

    src_ip:10.* => src_ip:[10.0.0.0 TO 10.255.255.255]

If the match ends in '.dat', then we attempt to read a file with that name and OR the condition:

    $ cat test.dat
    1.2.3.4
    1.2.3.5
    1.2.3.6
    1.2.3.7

We can source that file:

    src_ip:test.dat => src_ip:(1.2.3.4 OR 1.2.3.5 OR 1.2.3.6 OR 1.2.3.7)

This make it simple to use the --data-file output options and build queries based off previous queries.

=head2 Meta-Queries

Helpful in building queries is the --bases and --fields options which lists the index bases and fields:

    es-search.pl --bases

    es-search.pl --fields

    es-search.pl --base access --fields
