#!/usr/bin/env perl
# PODNAME: es-search.pl
# ABSTRACT: Provides a CLI for quick searches of data in ElasticSearch daily indexes
$|=1;           # Flush STDOUT
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use Carp;
use CLI::Helpers qw(:all);
use File::Slurp qw(slurp);
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use JSON;
use Pod::Usage;
use POSIX qw(strftime);
use YAML;

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT,
    'asc',
    'desc',
    'match-all',
    'exists:s',
    'missing:s',
    'prefix:s@',
    'size|n:i',
    'show:s',
    'top:s',
    'by:s',
    'tail',
    'fields',
    'bases',
    'all',
    'format:s',
    'no-header',
    'help|h',
    'manual|m',
);

# Search string is the rest of the argument string
my $search_string = join(' ', format_search_string(@ARGV));
my @query   = ();
if( defined $search_string && length $search_string ) {
    push @query, { query_string => { query => $search_string } };
}
elsif(exists $OPT{'match-all'} && $OPT{'match-all'}){
    push @query, {match_all =>{} };
}
if( exists $OPT{prefix} ){
    foreach my $prefix (@{ $OPT{prefix} }) {
        my ($f,$v) = split /:/, $prefix, 2;
        next unless $f && $v;
        push @query, { prefix => { $f => $v } };
    }
}

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitval => 0, -verbose => 2) if $OPT{manual};
my $unknown_options = join ', ', grep /^--/, @ARGV;
pod2usage({-exitval => 1, -msg =>"Unknown option(s): $unknown_options"}) if $unknown_options;

#--------------------------------------------------------------------------#
# App Config
my %CONFIG = (
    size   => (exists $OPT{size} && $OPT{size} > 0 ? int($OPT{size}) : 20),
    format => (exists $OPT{format} && length $OPT{format} ? lc $OPT{format} : 'yaml'),
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
if ( exists $OPT{show} && length $OPT{show} ) {
    @SHOW = grep { exists $FIELDS{$_} } split /,/, $OPT{show};
}
if( $OPT{bases} ) {
    show_bases();
    exit 0;
}
if( $OPT{fields} ) {
    show_fields();
    exit 0;
}
pod2usage({-exitval => 1, -msg => 'No search string specified'}) unless @query;
pod2usage({-exitval => 1, -msg => 'Cannot use --tail and --top together'}) if exists $OPT{tail} && $OPT{top};
pod2usage({-exitval => 1, -msg => 'Please specify --show with --tail'}) if exists $OPT{tail} && !@SHOW;

# Process extra parameters
my %extra   = ();
my @filters = ();
if( exists $OPT{exists} ) {
    foreach my $field (split /[,:]/, $OPT{exists}) {
        push @filters, { exists => { field => $field } };
    }
}
if( exists $OPT{missing} ) {
    foreach my $field (split /[,:]/, $OPT{missing}) {
        push @filters, { missing => { field => $field } };
    }
}
if( @filters ) {
    $extra{filter} = @filters > 1 ? { and => \@filters } : shift @filters;
}
my $DONE = 0;
local $SIG{INT} = sub { $DONE=1 };

my $top_type = exists $OPT{by} ? "aggregations" : "facets";
my %TOPKEYS = (
    aggregations => {
        base  => "buckets",
        key   => "key",
        count => "doc_count",
    },
    facets => {
        base  => "terms",
        key   => "term",
        count => "count",
    },
);
my %SUPPORTED_AGGREGATIONS = map {$_=>'simple_value'} qw(cardinality sum min max avg);
my $SUBAGG = undef;
my $facet_header = '';
if( exists $OPT{top} ) {
    my @facet_fields = grep { length($_) && exists $FIELDS{$_} } map { s/^\s+//; s/\s+$//; lc } split ',', $OPT{top};
    croak(sprintf("Option --top takes a field, found %d fields: %s\n", scalar(@facet_fields),join(',',@facet_fields)))
        unless @facet_fields == 1;

    my %sub_agg = ();
    if(exists $OPT{by}) {
        my ($type,$field) = split /\:/, $OPT{by};
        if( exists $SUPPORTED_AGGREGATIONS{$type} ) {
            $SUBAGG = $type;
            $sub_agg{by} = { $type => {field => $field} };
        }
        else {
            output({color=>'red'}, "Aggregation '$type' is not currently supported, ignoring.");
        }
    }

    my $facet = shift @facet_fields;
    $facet_header = "count\t" . $facet;
    $extra{$top_type} = { top => { terms => { field => $facet } } };

    if( $top_type eq 'facets' && exists $extra{filter} ) {
        $extra{$top_type}->{top}{facet_filter} = $extra{filter};
    }
    elsif( $top_type eq 'aggregations' && keys %sub_agg ) {
        $facet_header = "$OPT{by}\t" . $facet_header;
        $extra{$top_type}->{top}{terms}{order} = { by => $ORDER };
        $extra{$top_type}->{top}{aggregations} = \%sub_agg;
    }

    if( exists $OPT{all} ) {
        verbose({color=>'cyan'}, "Facets with --all are limited to returning 1,000,000 results.");
        $extra{$top_type}->{top}{terms}{size} = 1_000_000;
    }
    else {
        $extra{$top_type}->{top}{terms}{size} = $CONFIG{size};
    }
    $CONFIG{size} = 0;  # and we do not want any results other than the facet data
}
elsif(exists $OPT{tail}) {
    $CONFIG{size} = 20;
    @AGES = ($AGES[-1]);
}

my $size              = $CONFIG{size} > 50 ? 50 : $CONFIG{size};
my %displayed_indices = ();
my $TOTAL_HITS        = 0;
my $last_hit_ts       = undef;
my $duration          = 0;
my $displayed         = 0;
my $header            = exists $OPT{'no-header'};
my $age               = undef;
my %last_batch_id     = ();
my %FACET_TOTALS      = ();

AGES: while( !$DONE && @AGES ) {
    # With --tail, we don't want to deplete @AGES
    $age = $OPT{tail} ? $AGES[0] : shift @AGES;

    # Pause for 200ms if we're tailing
    select(undef,undef,undef,0.2) if exists $OPT{tail} && $last_hit_ts;

    my $start=time();
    $last_hit_ts ||= strftime('%Y-%m-%dT%H:%M:%S%z',localtime($start-30));

    # If we're tailing, bump the @query with a timestamp range
    push @query, {range => {'@timestamp' => {gte => $last_hit_ts}}} if $OPT{tail};

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
            query      => {
                bool => {
                    must => \@query,
                },
            },
            sort       => [ { '@timestamp' => $ORDER } ],
            %extra,
        }
    );
    $duration += time() - $start;

    # Remove the last searched date from the @query
    pop @query if exists $OPT{tail};

    # Advance if we don't have a result
    next unless defined $result;

    if ( $result->{error} ) {
        my ($simple_error) = $result->{error} =~ m/(QueryParsingException\[\[[^\]]+\][^\]]+\]\]);/;
        $simple_error ||= '';
        output({stderr=>1,color=>'red'},
            "# Received an error from the cluster. $simple_error"
        );
        last if $DONE;
        next;
    }
    $displayed_indices{$_} = 1 for @{ $by_age{$age} };
    $TOTAL_HITS += $result->{hits}{total} if $result->{hits}{total};

    my @always = qw(@timestamp);
    $header++ == 0 && @SHOW && output({color=>'cyan'}, join("\t", @always,@SHOW));
    while( $result && !$DONE ) {
        my $hits = ref $result->{hits}{hits} eq 'ARRAY' ? $result->{hits}{hits} : [];

        # Handle Faceting
        my $facets = exists $result->{$top_type} ? $result->{$top_type}{top}{$TOPKEYS{$top_type}->{base}} : [];
        if( @$facets ) {
            output({color=>'cyan'},$facet_header);
            foreach my $facet ( @$facets ) {
                $FACET_TOTALS{$facet->{$TOPKEYS{$top_type}->{key}}} ||= 0;
                $FACET_TOTALS{$facet->{$TOPKEYS{$top_type}->{key}}} += $facet->{$TOPKEYS{$top_type}->{count}};
                my @out = (
                    $facet->{$TOPKEYS{$top_type}->{count}},
                    $facet->{$TOPKEYS{$top_type}->{key}},
                );
                if(exists $facet->{by} ) {
                    if( exists $facet->{by}{value} ) {
                        unshift @out, $facet->{by}{value};
                    }
                }
                output(exists $OPT{by} ? {data=>1} : {}, join("\t",@out));
                $displayed++;
            }
            $TOTAL_HITS = exists $result->{$top_type}{top}{other} ? $result->{$top_type}{top}{other} + $displayed : $TOTAL_HITS;
            next AGES;
        }
        elsif(exists $result->{$top_type}{top}) {
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
                    $record->{$f} = exists $hit->{_source}{$f} ? $hit->{_source}{$f}
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
                        $v = ref $record->{$f} ? to_json($record->{$f},{allow_nonref=>1,canonical=>1}) : $record->{$f};
                    }
                    push @cols,$v;
                }
                $output = join("\t",@cols);
            }
            else {
                $output = $CONFIG{format} eq 'json' ? to_json($record,{allow_nonref=>1,canonical=>1,pretty=>1})
                        : Dump $record;
            }

            output({data=>1}, $output);
            $displayed++;
            last if all_records_displayed();
        }
        last if all_records_displayed();

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
    last if all_records_displayed();
}

output({stderr=>1,color=>'yellow'},
    "# Search Parameters:",
    (map { "#    " . to_json($_,{allow_nonref=>1}) } @query),
    "# Displaying $displayed of $TOTAL_HITS in $duration seconds.",
    sprintf("# Indexes (%d of %d) searched: %s\n",
            scalar(keys %displayed_indices),
            scalar(keys %indices),
            join(',', sort keys %displayed_indices)
    ),
);

if(!exists $OPT{by} && keys %FACET_TOTALS) {
    output({color=>'yellow'}, '#', '# Totals across batch', '#');
    output({color=>'cyan'},$facet_header);
    foreach my $k (sort { $FACET_TOTALS{$b} <=> $FACET_TOTALS{$a} } keys %FACET_TOTALS) {
        output({data=>1,color=>'green'},"$FACET_TOTALS{$k}\t$k");
    }
}

sub all_records_displayed {
    return 1 if $DONE;
    return 0 if exists $OPT{tail};
    return 0 if exists $OPT{all};
    return 1 if $displayed >= $CONFIG{size};
    return 0;
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
            if( defined $match && $match =~ /(.*\.(?:dat|csv|txt))(?:\[(-?\d+)(?:,(\d+))?\])?$/) {
                my($file,$col,$row) = ($1,$2,$3);
                if( -f $file ) {
                    my @rows = grep { defined && length } slurp($file);
                    $col //= -1; # Default to the last column
                    $row ||=  0; # Default to the first row
                    my $id = sprintf("FILE=%s:%s[%d,%d]", $term, $file, $col, $row);
                    if( $row > @rows ) {
                        verbose({color=>'yellow',sticky=>1}, "$id ROW $row is out-of-range, resetting to 0.");
                        $row = 0;
                    }
                    my @set = splice @rows, $row, 500;
                    if(@rows) {
                        verbose({color=>'yellow',sticky=>1}, sprintf '%s More than 500 rows, truncated, call with %s:%s[%d,%d] on next run.',
                            $id, $term, $file, $col, $row + 500
                        );
                    }
                    if( @set ) {
                        my %uniq=();
                        my @data=();
                        for(@set) {
                            my @cols = split /[\s,;]+/;
                            my $value = $cols[$col];
                            if(defined $value && !exists $uniq{$value}) {
                                push @data, $value;
                                $uniq{$value} = 1;
                            }
                        }
                        push @modified,"$term:(" . join(' ', @data) . ")";
                        next;
                    }
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
    --by                Perform an aggregation using the result of this, example: --by cardinality:@fields.src_ip
    --match-all         Enables the ElasticSearch match_all operator
    --prefix            Takes "field:string" and enables the Lucene prefix query for that field
    --exists            Field which must be present in the document
    --missing           Field which must not be present in the document
    --size              Result size, default is 20
    --all               Don't consider result size, just give me *everything*
    --asc               Sort by ascending timestamp
    --desc              Sort by descending timestamp (Default)
    --format            When --show isn't used, use this method for outputting the record, supported: json, yaml
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

=item B<format>

Output format to use when the full record is dumped.  The default is 'yaml', but 'json' is also supported.

    --format json

=item B<tail>

Repeats the query every second until CTRL+C is hit, displaying new results.  Due to the implementation,
this mode enforces that only the most recent indices are searched.  Also, given the output is continuous, you must
specify --show with this option.

=item B<top>

Perform an aggregation or facet returning the top field.  Limited to a single field at this time.
This option is not available when using --tail.

    --top src_ip

=item B<by>

Perform a sub aggregation on the top terms aggregation and order by the result of this aggregation.
Aggregation syntax is as follows:

    --by <type>:<field>

A full example might look like this:

    $ es-search.pl --base access dst:www.example.com --top src_ip --by cardinality:@fields.acct

This will show the top source IP's ordered by the cardinality (count of the distinct values) of accounts logging
in as each source IP, instead of the source IP with the most records.

Supported sub agggregations and formats:

    cardinality:<field>
    min:<field>
    max:<field>
    avg:<field>
    sum:<field>


=item B<match-all>

Apply the ElasticSearch "match_all" search operator to query on all documents in the index.

=item B<prefix>

Takes a "field:string" combination and you can use multiple --prefix options will be "AND"'d

Example:

    --prefix useragent:'Go '

Will search for documents where the useragent field matches a prefix search on the string 'Go '

JSON Equivalent is:

    { "prefix": { "useragent": "Go " } }

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

If the match ends in .dat, .txt, or .csv, then we attempt to read a file with that name and OR the condition:

    $ cat test.dat
    50 1.2.3.4
    40 1.2.3.5
    30 1.2.3.6
    20 1.2.3.7

Or

    $ cat test.csv
    50,1.2.3.4
    40,1.2.3.5
    30,1.2.3.6
    20,1.2.3.7

Or

    $ cat test.txt
    1.2.3.4
    1.2.3.5
    1.2.3.6
    1.2.3.7

We can source that file:

    src_ip:test.dat => src_ip:(1.2.3.4 1.2.3.5 1.2.3.6 1.2.3.7)

This make it simple to use the --data-file output options and build queries based off previous queries.

You can also specify the column of the data file to use, the default being the last column or (-1).  Columns are
B<zero-based> indexing. This means the first column is index 0, second is 1, ..  The previous example can be rewritten
as:

    src_ip:test.dat[1]

or:
    src_ip:test.dat[-1]

It is also possible to use a B<zero-based> row offset in the specification.  For example:

    src_ip:test.dat[-1,2]

Yields a search string of

    src_ip:test.dat => src_ip:(1.2.3.6 1.2.3.7)

This option only supports 500 elements in a list.

=head2 Meta-Queries

Helpful in building queries is the --bases and --fields options which lists the index bases and fields:

    es-search.pl --bases

    es-search.pl --fields

    es-search.pl --base access --fields
