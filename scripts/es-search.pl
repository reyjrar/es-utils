#!perl
# PODNAME: es-search.pl
# ABSTRACT: Provides a CLI for quick searches of data in ElasticSearch daily indexes
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use App::ElasticSearch::Utilities::Query;
use App::ElasticSearch::Utilities::QueryString;
use Carp;
use CLI::Helpers qw(:all);
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use JSON::MaybeXS qw(:legacy);
use Pod::Usage;
use POSIX qw(strftime);
use Ref::Util qw(is_ref is_arrayref is_hashref);
use Time::HiRes qw(sleep time);
use YAML;

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT, qw(
    all
    asc
    bases
    bg-filter=s
    by=s
    desc
    exists=s
    fields
    filter
    format=s
    help|h
    json|jq
    manual|m
    match-all
    max-batch-size=i
    missing=s
    no-decorators|no-header
    no-implications|no-imply
    prefix=s@
    pretty
    show=s@
    size|n|limit=i
    sort=s
    tail
    timestamp=s
    top=s
    interval=s
    with=s@
    or
));

# Search string is the rest of the argument string
my $context = $OPT{filter} ? 'filter' : 'must';
my $qs = App::ElasticSearch::Utilities::QueryString->new(
            $OPT{filter} ?  (context => 'filter') : (),
            default_join => $OPT{or} ? 'OR' : 'AND',
);
my $q = exists $OPT{'match-all'} && $OPT{'match-all'}
            ? App::ElasticSearch::Utilities::Query->new($context => { match_all => {} })
            : $qs->expand_query_string(@ARGV);

$q->set_timeout('10s');
$q->set_scroll('30s');

if( exists $OPT{prefix} ){
    foreach my $prefix (@{ $OPT{prefix} }) {
        my ($f,$v) = split /:/, $prefix, 2;
        next unless $f && $v;
        $q->add_bool( $context => { prefix => { $f => $v } } );
    }
}

#------------------------------------------------------------------------#
# Documentation
pod2usage({-sections => 'SYNOPSIS'}) if $OPT{help};
pod2usage(-exitval => 0, -verbose => 2) if $OPT{manual};
my $unknown_options = join ', ', grep /^--/, @ARGV;
pod2usage({-exitval => 1, -sections => 'SYNOPSIS', -msg =>"Unknown option(s): $unknown_options"}) if $unknown_options;

#--------------------------------------------------------------------------#
# Information Gathering Routines
if( $OPT{bases} ) {
    show_bases();
    exit 0;
}
#--------------------------------------------------------------------------#
# App Config
my %CONFIG = (
    size      => ($OPT{size} && $OPT{size} > 0) ? int($OPT{size}) : 20,
    format    => $OPT{json}   ? 'json'
               : $OPT{format} ? lc $OPT{format}
               : 'yaml',
    'max-batch-size' => $OPT{'max-batch-size'} || 50,
    $OPT{timestamp} ? ( timestamp => $OPT{timestamp} ) : (),
);
$OPT{'no-decorators'} = 1 if $CONFIG{format} eq 'json';
$CONFIG{pretty} = $OPT{pretty} ? 1
                : $CONFIG{format} =~ /pretty/ ? 1
                : 0;
#------------------------------------------------------------------------#
# Handle Indices
my $ORDER = exists $OPT{asc} && $OPT{asc} ? 'asc' : 'desc';
$ORDER = 'asc' if exists $OPT{tail};
my %by_age = ();
my %indices = map { $_ => (es_index_days_old($_) || 0) } es_indices();
die "# Failed to retrieve any indices using your paramaters." unless keys %indices;
my %FIELDS = ();
my $TimeStampCheck=0;
foreach my $index (sort by_index_age keys %indices) {
    my $age = $indices{$index};
    $by_age{$age} ||= [];
    push @{ $by_age{$age} }, $index;
    my $fields = es_index_fields($index);
    foreach my $k ( keys %{ $fields } ) {
        $FIELDS{$k} = $fields->{$k}
            unless $FIELDS{$k};
    }
    # Lookup the Index in our local YAML
    if( !$TimeStampCheck ) {
        $TimeStampCheck++;
        $CONFIG{timestamp} ||= es_local_index_meta(timestamp => $index);
    }
}

# Set fields so we know how to construct complex aggs
$q->fields_meta( \%FIELDS );

#------------------------------------------------------------------------#
# Figure out the timestamp
$CONFIG{timestamp} ||= es_globals('timestamp') || '@timestamp';
debug_var(\%by_age);
my @AGES = sort { $ORDER eq 'asc' ? $b <=> $a : $a <=> $b } keys %by_age;
# Figure out if we summarize
$CONFIG{summary} = @AGES > 1 && $OPT{top} && ( !$OPT{by} && !$OPT{with} && !$OPT{interval} );
debug({color=>"cyan"}, "Fields discovered.");

if( $OPT{fields} ) {
    show_fields();
    exit 0;
}
# Attempt date autodiscovery
if( !exists $FIELDS{$CONFIG{timestamp}} ) {
    my @dates = grep { $FIELDS{$_}->{type} eq 'date' } keys %FIELDS;
    if( @dates == 0 ) {
        output({color=>'red',stderr=>1},"FATAL: No date fields found in the indices specified" );
        exit 1;
    }
    elsif( @dates == 1 ) {
        output({color=>'yellow',stderr=>1}, "WARNING: Timestamp field '$CONFIG{timestamp}' not found, using '$dates[0]' instead");
        $CONFIG{timestamp} = $dates[0];
    }
    else {
        output({color=>'red',stderr=>1},
            sprintf "FATAL: Timestamp field '%s' not found and discovered multiple date fields: %s",
                $CONFIG{timestamp},
                join(', ', sort @dates)
        );
        output({color=>'yellow',indent=>1}, "Try again with '--timestamp $dates[0]' for example.");
        exit 1;
    }
}

# Which fields to show
my @SHOW = ();
if ( exists $OPT{show} && scalar @{ $OPT{show} } ) {
    foreach my $args (@{ $OPT{show} }) {
        push @SHOW, grep { defined && length } split /,/, $args;
    }
}
# How to sort
my $SORT = [ { $CONFIG{timestamp} => $ORDER } ];
if( exists $OPT{sort} && length $OPT{sort} ) {
    $SORT = [
        map { /:/ ? +{ split /:/ } : $_ }
        split /,/,
        $OPT{sort}
    ]
}
$q->set_sort($SORT);

# Improper Usage
pod2usage({-exitval=>1, -verbose=>0, -sections=>'SYNOPSIS', -msg=>'No search string specified'})
    unless keys %{ $q->query };
pod2usage({-exitval=>1, -verbose=>0, -sections=>'SYNOPSIS', -msg=>'Cannot use --tail and --top together'})
    if exists $OPT{tail} && $OPT{top};
pod2usage({-exitval=>1, -verbose=>0, -sections=>'SYNOPSIS', -msg=>'Cannot use --tail and --sort together'})
    if exists $OPT{tail} && $OPT{sort};
pod2usage({-exitval=>1, -verbose=>0, -sections=>'SYNOPSIS', -msg=>'Cannot use --sort along with --asc or --desc'})
    if $OPT{sort} && ($OPT{asc} || $OPT{desc});
pod2usage({-exitval=>1, -verbose=>0, -sections=>'SYNOPSIS', -msg=>'Please specify --show with --tail'})
    if exists $OPT{tail} && !@SHOW;

# Process extra parameters
if( exists $OPT{exists} ) {
    foreach my $field (split /[,:]/, $OPT{exists}) {
        $q->add_bool( $context => { exists => { field => $field } } );
    }
}
if( exists $OPT{missing} ) {
    foreach my $field (split /[,:]/, $OPT{missing}) {
        $q->add_bool( must_not => { exists => { field => $field } } );
    }
}

my %SUPPORTED_AGGREGATIONS = map {$_=>'simple_value'} qw(cardinality sum min max avg);
my $SUBAGG = undef;
my $agg_header = '';
if( exists $OPT{top} ) {
    my @top = split /:/, $OPT{top};
    my $top_field = pop @top;
    my $top_agg   = @top ? shift @top : 'terms';

    my @agg_fields = grep { exists $FIELDS{$_} } split /\s*,\s*/, $top_field;
    croak(sprintf("Option --top takes a field, found %d fields: %s\n", scalar(@agg_fields),join(',',@agg_fields)))
        unless @agg_fields == 1;

    my %agg     = ();
    my %sub_agg = ();
    if( $OPT{by}) {
        my ($type,$field) = split /\:/, $OPT{by};
        if( exists $SUPPORTED_AGGREGATIONS{$type} ) {
            $SUBAGG = $type;
            $sub_agg{by} = { $type => {field => $field} };
        }
        else {
            output({color=>'red'}, "Aggregation '$type' is not currently supported, ignoring.");
        }
    }
    if( $OPT{with} ) {
        my @with = is_arrayref($OPT{with}) ? @{ $OPT{with} } : ( $OPT{with} );
        foreach my $with ( @with )  {
            my @attrs = split /:/, $with, 3;
            # Process Args from Right to Left
            my $arg   = @attrs == 3 ? pop @attrs
                      : $attrs[-1] =~ /^\d/ ? pop @attrs
                      : '';
            my $pcts  = $arg =~ /^\d{1,2}(?:\.\d+)?(?:,\d{1,2}(?:\.\d+)?)*$/ ? $arg : '25,50,75,90,95,99';
            my $size  = $arg =~ /^\d+$/ ? $arg : 3;
            my $hi    = $arg || 0.1;
            my $field = exists $FIELDS{$attrs[-1]} ? pop @attrs : undef;
            my $type  = @attrs ? pop @attrs : 'terms';
            # Skip invalid elements
            next unless defined $field and defined $size and $size > 0;

            my $id = "$type.$field";
            # If a term agg and we haven't used this field name, simplify it
            if( $type =~ /terms$/ && !$sub_agg{$field} ) {
                $id = $field;
            }

            if( $type =~ /histogram|stats|percentiles/ && !$OPT{'no-implications'} ) {
                output({color=>'magenta',sticky=>1}, "* Using a statistical aggregation implies an exists filter on $field, use --no-implications to disable this");
                $q->add_bool( must => { exists => { field => $field } } );
            }

            $sub_agg{$id} = {
                $type => {
                    field => $field,
                    $type =~ /terms/ ? (size  => $size) : (),
                    $type eq 'percentiles' ? ( percents => [split /,/, $pcts] ) : (),
                    $type eq 'histogram'   ? ( interval => $hi ) :  (),
                }
            };
        }
    }

    my $field = shift @agg_fields;
    $agg_header = "count\t" . $field;
    $agg{$top_agg} = { field => $field };

    if( $OPT{'bg-filter'} && $top_agg eq 'significant_terms' ) {
        my $bgf = App::ElasticSearch::Utilities::QueryString->new();
        my $bgq = $bgf->expand_query_string($OPT{'bg-filter'});
        $agg{$top_agg}->{background_filter} = $bgq->query;

    }

    if( exists $sub_agg{by} ) {
        $agg_header = "$OPT{by}\t" . $agg_header;
        $agg{$top_agg}->{order} = { by => $ORDER };
    }
    $agg{aggregations} = \%sub_agg if keys %sub_agg;

    if( exists $OPT{all} ) {
        verbose({color=>'cyan'}, "# Aggregations with --all are limited to returning 1,000,000 results.");
        $agg{$top_agg}->{size} = 1_000_000;
    }
    else {
        $agg{$top_agg}->{size} = $CONFIG{size};
    }
    $q->add_aggregations( top => \%agg );
    $q->add_aggregations( out_of => { cardinality => { field => $field  } } );

    if( $OPT{interval} ) {
        $q->wrap_aggregations( step => {
            date_histogram => {
                field    => $CONFIG{timestamp},
                interval => $OPT{interval},
            }
        });
    }
}
elsif(exists $OPT{tail}) {
    $q->set_size($CONFIG{'max-batch-size'});
    @AGES = ($AGES[-1]);
}
else {
    $q->set_size( $CONFIG{size} < $CONFIG{'max-batch-size'} ? $CONFIG{size} : $CONFIG{'max-batch-size'} );
}

my %displayed_indices = ();
my $TOTAL_HITS        = 0;
my $OUT_OF            = 0;
my $last_hit_ts       = undef;
my $duration          = 0;
my $displayed         = 0;
my $header            = 0;
my $age               = undef;
my %last_batch_id     = ();
my %AGGS_TOTALS       = ();
my %AGES_SEEN         = ();
# Handle CTRL+C During the Loop
my $DONE              = 0;
local $SIG{INT}       = sub { $DONE=1 };

verbose({color=>'green',level=>1}, "= Query setup complete, beginning request.");
AGES: while( !$DONE && @AGES ) {
    # With --tail, we don't want to deplete @AGES
    $age = $OPT{tail} ? $AGES[0] : shift @AGES;

    # Pause for 200ms if we're tailing
    sleep(0.2) if exists $OPT{tail} && $last_hit_ts;

    my $start=time();
    $last_hit_ts ||= strftime('%Y-%m-%dT%H:%M:%S%z',localtime($start-30));

    # If we're tailing, bump the @query with a timestamp range
    $q->stash( filter => {range => { $CONFIG{timestamp} => {gte => $last_hit_ts}}} ) if $OPT{tail};

    # Header
    if( !exists $AGES_SEEN{$age} ) {
        output({color=>'yellow'}, "= Querying Indexes: " . join(',', @{ $by_age{$age} })) unless $OPT{'no-decorators'};
        $AGES_SEEN{$age}=1;
        $header=0;
    }

    debug("== Query");
    debug_var($q->request_body);
    debug_var($q->uri_params);

    my $result = es_request('_search',
        # Search Parameters
        {
            index     => $by_age{$age},
            uri_param => $q->uri_params,
            method    => 'POST',
        },
        # Search Body
        $q->request_body,
    );
    debug({clear=>1},"== Results");
    debug_var($result);
    $duration += time() - $start;

    # Advance if we don't have a result
    next unless defined $result;

    if ( $result->{error} ) {
        my $simple_error;
        eval {
            $simple_error = $result->{error}{caused_by}{caused_by}{reason};
        } or do {
            ($simple_error) = $result->{error} =~ m/(QueryParsingException\[\[[^\]]+\][^\]]+\]\]);/;
        };
        $simple_error ||= '';
        output({stderr=>1,color=>'red'},
            "# Received an error from the cluster. $simple_error"
        );
        last;
    }
    $displayed_indices{$_} = 1 for @{ $by_age{$age} };
    $TOTAL_HITS += $result->{hits}{total} if $result->{hits}{total};

    my @always = ($CONFIG{timestamp});
    if(!$OPT{'no-decorators'} && !$header && @SHOW) {
        output({color=>'cyan'}, join("\t", @always,@SHOW));
        $header++;
    }

    while( $result && !$DONE ) {
        my $hits = is_arrayref($result->{hits}{hits}) ? $result->{hits}{hits} : [];

        # Handle Aggregations
        if( exists $result->{aggregations} ) {
            my $out_of =  $result->{aggregations}{out_of}{value};
            $OUT_OF = $out_of if $out_of > $OUT_OF;
            my $steps = exists $result->{aggregations}{step} ? $result->{aggregations}{step}{buckets}
                      : [ $result->{aggregations} ];
            my $indent = exists $result->{aggregations}{step} ? 1 : 0;
            foreach my $step ( @$steps ) {
                my $aggs = exists $step->{top} ? $step->{top}{buckets} : [];
                if( exists $step->{key_as_string} ) {
                    output({color=>'cyan',clear=>1}, sprintf "%d\t%s", @{$step}{qw(doc_count key_as_string)});
                }
                if( @$aggs ) {
                    # For top the N of T needs to represent maximums
                    $displayed = scalar(@$aggs) if scalar(@$aggs) > $displayed;
                    output({color=>'cyan',indent=>$indent},$agg_header) unless $OPT{'no-decorators'};
                    foreach my $agg ( @$aggs ) {
                        $AGGS_TOTALS{$agg->{key}} ||= 0;
                        $AGGS_TOTALS{$agg->{key}} += $agg->{doc_count};
                        my @out = ();

                        foreach my $k (qw(score doc_count bg_count key)) {
                            next unless exists $agg->{$k};
                            my $value = delete $agg->{$k};
                            push @out, defined $value ? ($k eq 'score' ? sprintf "%0.3f", $value : $value ) : '-';
                        }
                        if(exists $agg->{by} ) {
                            my $by = delete $agg->{by};
                            if( exists $by->{value} ) {
                                unshift @out, $by->{value};
                            }
                        }
                        # Handle the --with elements
                        my %subaggs = ();
                        if( keys %{ $agg } ) {
                            foreach my $k (sort keys %{ $agg }) {
                                next unless is_hashref($agg->{$k});
                                if( exists $agg->{$k}{buckets} ) {
                                    my @sub;
                                    foreach my $subagg (@{ $agg->{$k}{buckets} }) {
                                        my @elms = ();
                                        next unless exists $subagg->{key};
                                        push @elms, $subagg->{key};
                                        foreach my $dk (qw(score doc_count bg_count)) {
                                            next unless exists $subagg->{$dk};
                                            my $v = delete $subagg->{$dk};
                                            push @elms, defined $v ? ($dk eq 'score' ? sprintf "%0.3f", $v : $v ) : '-';
                                        }
                                        push @sub, \@elms;
                                    }
                                    $subaggs{$k} = \@sub if @sub;
                                }
                                # Simple Numeric Aggs
                                elsif( $agg->{$k}{value} ) {
                                    $subaggs{$k} = [ [ $agg->{$k}{value} ] ];
                                }
                                # Percentiles
                                elsif( $agg->{$k}{values} ) {
                                    my @pcts;
                                    foreach my $pctl (sort { $a <=> $b } keys %{ $agg->{$k}{values} }) {
                                        push @pcts, "p$pctl", $agg->{$k}{values}{$pctl};
                                    }
                                    $subaggs{$k} = [ \@pcts ];
                                }
                                # Statistics
                                elsif( $agg->{$k}{avg} ) {
                                    my @stats;
                                    my %alias = qw( variance var std_deviation stdev );
                                    foreach my $stat (qw(count min avg max sum variance std_deviation)) {
                                        next unless exists $agg->{$k}{$stat};
                                        my $v = $agg->{$k}{$stat} =~ /\./ ? sprintf "%0.3f", $agg->{$k}{$stat}
                                                                          : $agg->{$k}{$stat};
                                        push @stats, $alias{$stat} || $stat => $v;
                                    }
                                    $subaggs{$k} = [ \@stats ];
                                }
                            }
                        }
                        if( keys %subaggs ) {
                            foreach my $subagg (sort keys %subaggs) {
                                foreach my $extra ( @{ $subaggs{$subagg} } ) {
                                    output({indent=>$indent,data=>1},
                                        join "\t", @out, $subagg, @{ $extra }
                                    );
                                }
                            }
                        }
                        else {
                            # Simple output
                            output({indent=>$indent,data=>!$CONFIG{summary}}, join("\t",@out));
                        }
                    }
                }
                elsif(exists $result->{aggregations}{top}) {
                    output({indent=>1,color=>'red'}, "= No results.");
                }
            }
            next AGES;
        }

        # Reset the last batch ID if we have new data
        %last_batch_id = () if @{$hits} > 0 && $last_hit_ts ne $hits->[-1]->{_source}{$CONFIG{timestamp}};
        debug({color=>'magenta'}, "+ ID cache is now empty.") unless keys %last_batch_id;

        foreach my $hit (@{ $hits }) {
            # Skip if we've seen this record
            next if exists $last_batch_id{$hit->{_id}};

            $last_hit_ts = $hit->{_source}{$CONFIG{timestamp}};
            $last_batch_id{$hit->{_id}}=1;
            my $record = {};
            if( @SHOW ) {
                my $flat = es_flatten_hash( $hit->{_source} );
                debug_var($flat);
                foreach my $f (@always) {
                    $record->{$f} = $flat->{$f};
                }
                foreach my $f (@SHOW) {
                    my $value = undef;
                    if( exists $flat->{$f} ) {
                        $value = $flat->{$f};
                    }
                    elsif( my $v = document_lookdown($hit->{_source},$f) ) {
                        $value = $v;
                    }
                    elsif(index($f, '.') > 0) {
                        # Try path matching the key
                        my @values = ();
                        foreach my $k (keys %{ $flat }) {
                            if( index($k,$f) == 0 ) {
                                push @values, $flat->{$k};
                            }
                            elsif( $k =~ /\.\d+\./ ) {
                                my $flatter =  join '.', grep { !/^\d+$/ } split /\./, $k;
                                if ( $flatter eq $f ) {
                                    push @values, $flat->{$k};
                                }
                            }
                        }
                        $value = @values ? @values == 1 ? $values[0] : \@values : undef;
                    }
                    $record->{$f} = $value;
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
                        $v = is_arrayref($record->{$f}) && @{ $record->{$f} } == 1 ? $record->{$f}[0]
                           : is_ref($record->{$f}) ? to_json($record->{$f},{allow_nonref=>1,canonical=>1})
                           : $record->{$f};
                    }
                    push @cols,$v;
                }
                $output = join("\t",@cols);
            }
            else {
                $output = $CONFIG{format} =~ /^json/? to_json($record,{allow_nonref=>1,canonical=>1,pretty=>$CONFIG{pretty}})
                        : Dump $record;
            }

            output({data=>1}, $output);
            $displayed++;
            last if all_records_displayed();
        }
        last if all_records_displayed();

        # Scroll forward
        $start = time;
        $result = es_request('_search/scroll',
            {
                method => 'POST',
            },
            {
                scroll => $q->scroll,
                scroll_id => $result->{_scroll_id},
            }
        );
        $duration += time - $start;
        last unless $result->{hits} && $result->{hits}{hits} && @{ $result->{hits}{hits} } > 0
    }
    last if all_records_displayed();
}

output({stderr=>1,color=>'yellow'},
    "# Search Parameters:",
    (map { "#    $_" } split /\r?\n/, to_json($q->query,{allow_nonref=>1,canonical=>1,pretty=>$CONFIG{pretty}})),
    sprintf("# Displaying %d of %d results%s took %0.2f seconds.",
        $displayed,
        $OUT_OF || $TOTAL_HITS,
        $OUT_OF ? " in $TOTAL_HITS documents" : '',
        $duration,
    ),
    sprintf("# Indexes (%d of %d) searched: %s\n",
            scalar(keys %displayed_indices),
            scalar(keys %indices),
            join(',', sort keys %displayed_indices)
    ),
) unless $OPT{'no-decorators'};

if($CONFIG{summary} && keys %AGGS_TOTALS) {
    unless ( $OPT{'no-decorators'} ) {
        output({color=>'yellow'}, '#', '# Totals across batch', '#');
        output({color=>'cyan'},$agg_header);
    }
    foreach my $k (sort { $AGGS_TOTALS{$b} <=> $AGGS_TOTALS{$a} } keys %AGGS_TOTALS) {
        output({data=>1,color=>'green'},"$AGGS_TOTALS{$k}\t$k");
    }
}

sub all_records_displayed {
    return 1 if $DONE;
    return 0 if exists $OPT{tail};
    return 0 if exists $OPT{all};
    return 1 if $displayed >= $CONFIG{size};
    return 0;
}

sub document_lookdown {
    my ($href,$field) = @_;

    return $href->{$field} if exists $href->{$field};

    foreach my $k (keys %{ $href }) {
        if( is_hashref($href->{$k}) ) {
            return document_lookdown($href->{$k},$field);
        }
    }
    return;
}

sub show_fields {
    output({color=>'cyan'}, 'Fields available for search:' );
    my $total = 0;
    my %types = ();
    foreach my $field (sort keys %FIELDS) {
        $total++;
        my $type = $FIELDS{$field}->{type};
        $types{$type} ||= 0;
        $types{$type}++;
        my $color = $type eq 'ip' ? 'magenta'
                  : $type eq 'text' ? 'red'
                  : $type =~ /float|integer|short|byte|double/ ? 'cyan'
                  : $type =~ /^geo/ ? 'green'
                  : $type =~ /^date/ ? 'yellow'
                  : 'white';
        output({indent=>1,kv=>1,color=>$color}, $field => $type);
        output({indent=>2}, sprintf "nested: %s - %s",
                @{ $FIELDS{$field} }{qw(nested_path nested_key)}
        ) if exists $FIELDS{$field}->{nested_path};
    }
    output({color=>"yellow"},
        sprintf("# Fields: %d from a combined %d indices.\n",
            $total,
            scalar(keys %indices),
        )
    );
    # Type Meta Roll Up
    output({indent=>1}, join(', ',
            map  { "$types{$_} $_ fields" }
            sort { $types{$b} <=> $types{$a} }
            keys %types
        )
    );
}

sub show_bases {
    output({color=>'cyan'}, 'Bases available for search:' );
    my @all   = es_indices(_all => 1);
    my %bases = ();

    foreach my $index (@all) {
        next if $index =~ /^\./;
        my $days_old = es_index_days_old( $index ) || 0;
        next unless defined $days_old;
        $days_old = 0 if $days_old < 0;
        foreach my $base (es_index_bases($index)) {
            if( exists $bases{$base} ) {
                $bases{$base}->{oldest}   = $days_old if $days_old > $bases{$base}->{oldest};
                $bases{$base}->{youngest} = $days_old if $days_old < $bases{$base}->{youngest};
            }
            else {
                $bases{$base} = { oldest => $days_old, youngest => $days_old };
            }
        }
    }
    foreach my $base (sort keys %bases) {
        output({indent=>1,color=>'green'},$base);
        verbose({indent=>2,kv=>1},
            map {
                $_ => sprintf "%d days old", $bases{$base}->{$_}
            } qw( youngest oldest )
        );
    }

    output({color=>"yellow"},
        sprintf("# Bases: %d from a combined %d indices.\n",
            scalar(keys %bases),
            scalar(@all),
        )
    );
}

sub by_index_age {
    return $ORDER eq 'asc'
        ? $indices{$b} <=> $indices{$a}
        : $indices{$a} <=> $indices{$b};
}
__END__

=head1 NAME

es-search.pl - Search a logging cluster for information

=head1 SYNOPSIS

es-search.pl [search string]

Options:

    --help              print help
    --manual            print full manual
    --filter            Force filter context for all query elements
    --show              Comma separated list of fields to display, default is ALL, switches to tab output
    --tail              Continue the query until CTRL+C is sent
    --top               Perform an aggregation on the fields, by a comma separated list of up to 2 items
    --by                Perform an aggregation using the result of this, example: --by cardinality:src_ip
    --with              Perform a sub aggregation on the query
    --bg-filter         Only used if --top aggregation is significant_terms, applies a background filter
    --match-all         Enables the ElasticSearch match_all operator
    --interval          When running aggregations, wrap the aggreation in a date_histogram with this interval
    --prefix            Takes "field:string" and enables the Lucene prefix query for that field
    --exists            Field which must be present in the document
    --missing           Field which must not be present in the document
    --size              Result size, default is 20, aliased to -n and --limit
    --all               Don't consider result size, just give me *everything*
    --asc               Sort by ascending timestamp
    --desc              Sort by descending timestamp (Default)
    --sort              List of fields for custom sorting
    --format            When --show isn't used, use this method for outputting the record, supported: json, jsonpretty, yaml
                        json assumes --no-decorator as we assume you're piping through jq
    --pretty            Where possible, use JSON->pretty
    --no-decorators     Do not show the header with field names in the query results
    --no-header         Same as above
    --no-implications   Don't attempt to imply filters from statistical aggregations
    --fields            Display the field list for this index!
    --bases             Display the index base list for this cluster.
    --timestamp         Field to use as the date object, default: @timestamp

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=item B<filter>

Forces filter context for all query parameters, the default is using query context.

=item B<show>

Comma separated list of fields to display in the dump of the data

    --show src_ip,crit,file,out_bytes

=item B<sort>

Use this option to sort your documents on fields other than the timestamp. Fields are given as a comma separated list:

    --sort field1,field2

To specify per-field sort direction use:

    --sort field1:asc,field2:desc

Using this option together with C<--asc>, C<--desc> or C<--tail> is not possible.

=item B<format>

Output format to use when the full record is dumped.  The default is 'yaml', but 'json' is also supported.

    --format json

=item B<tail>

Repeats the query every second until CTRL+C is hit, displaying new results.  Due to the implementation,
this mode enforces that only the most recent indices are searched.  Also, given the output is continuous, you must
specify --show with this option.

=item B<top>

Perform an aggregation returning the top field.  Limited to a single field at this time.
This option is not available when using --tail.

    --top src_ip

You can override the default of the C<terms> bucket aggregation by prefixing
the parameter with the required bucket aggregation, i.e.:

    --top significant_terms:src_ip

=item B<by>

Perform a sub aggregation on the top terms aggregation and order by the result of this aggregation.
Aggregation syntax is as follows:

    --by <type>:<field>

A full example might look like this:

    $ es-search.pl --base access dst:www.example.com --top src_ip --by cardinality:acct

This will show the top source IP's ordered by the cardinality (count of the distinct values) of accounts logging
in as each source IP, instead of the source IP with the most records.

Supported sub agggregations and formats:

    cardinality:<field>
    min:<field>
    max:<field>
    avg:<field>
    sum:<field>

=item B<with>

Perform a subaggregation on the top terms and report that sub aggregation details in the output.  The format is:

    --with <aggregation>:<field>:<size>

The default B<size> is 3.
The default B<aggregation> is 'terms'.

B<field> is the only required element.

e.g.

    $ es-search.pl --base logstash error --top program --size 2 --by cardinality:host --with host:5

This will show the top 2 programs with log messages containing the word error by the cardinality (count
distinct host) of hosts showing the top 5 hosts

Without the --with, the results might look like this:

    112314 sshd
    21224  ntp

The B<--with> option would expand that output to look like this:

    112314   host   bastion-804   12431   sshd
    112314   host   bastion-803   10009   sshd
    112314   host   bastion-805   9768    sshd
    112314   host   bastion-801   8789    sshd
    112314   host   bastion-802   4121    sshd
    21224    host   webapp-324    21223   ntp
    21224    host   mail-42       1       ntp

This may be specified multiple times, the result is more I<rows>, not more I<columns>, e.g.


    $ es-search.pl --base logstash error --top program --size 2 --by cardinality:host --with host:5 --with dc:2

Produces:

    112314   dc     arlington     112314  sshd
    112314   host   bastion-804   12431   sshd
    112314   host   bastion-803   10009   sshd
    112314   host   bastion-805   9768    sshd
    112314   host   bastion-801   8789    sshd
    112314   host   bastion-802   4121    sshd
    21224    dc     amsterdam     21223   ntp
    21224    dc     la            1       ntp
    21224    host   webapp-324    21223   ntp
    21224    host   mail-42       1       ntp

You may sub aggregate using any L<bucket agggregation|https://www.elastic.co/guide/en/elasticsearch/reference/master/search-aggregations-bucket.html>
as long as the aggregation provides a B<key> element.  Additionally, doc_count, score, and bg_count will be reported in the output.

Other examples:

    --with significant_terms:crime
    --with cardinality:accts
    --with min:out_bytes
    --with max:out_bytes
    --with avg:out_bytes
    --with sum:out_bytes
    --with stats:out_bytes
    --with extended_stats:out_bytes
    --with percentiles:out_bytes
    --with percentiles:out_bytes:50,95,99
    --with histogram:out_bytes:1024

=item B<bg-filter>

Only used if the C<--top> aggregation is C<significant_terms>.  Sets the
background filter for the C<significant_terms> aggregation.

    es-search.pl --top significant_terms:src_ip method:POST file:\/get\/sensitive_data --bg-filter method:POST

=item B<interval>

When performing aggregations, wrap those aggregations in a date_histogram of this interval.  This
helps flush out "what changed in the last hour."

=item B<match-all>

Apply the ElasticSearch "match_all" search operator to query on all documents
in the index.  This is the default with no search parameters.

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

Use with --verbose to show age information on the indexes in each base.

=item B<fields>

Display a list of searchable fields

=item B<index>

Search only this index for data, may also be a comma separated list

=item B<days>

The number of days back to search, the default is 5

=item B<base>

Index base name, will be expanded using the days back parameter.  The default
is 'logstash' which will expand to 'logstash-YYYY.MM.DD'

=item B<timestamp>

The field in your documents that we'll treat as a "date" type in our queries.

May also be specified in the C<~/.es-utils.yaml> file per index, or index base:

    ---
    host: es-readonly-01
    port: 9200
    meta:
      bro:
        timestamp: 'record_ts'
      mayans-2012.12.21:
        timestamp: 'end_of_the_world'

Then running:

    # timestamp is set to '@timestamp', the default
    es-search.pl --base logstash --match-all

    # timestamp is set to 'record_ts', from ~/.es-utils.yaml
    es-search.pl --base bro --match-all

    # timestamp is set to '@timestamp', the default
    es-search.pl --base mayans --match-all

    # timestamp is set to 'end_of_the_world', from ~/.es-utils.yaml
    es-search.pl --index mayans-2012.12.21 --match-all

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

    # Search for all apache logs past with status 500
    es-search.pl program:"apache" AND crit:500

    # Search for all apache logs with status 500 show only file and out_bytes
    es-search.pl program:"apache" AND crit:500 --show file,out_bytes

    # Search for ip subnet client IP 1.2.3.0 to 1.2.3.255 or 1.2.0.0 to 1.2.255.255
    es-search.pl --size=100 dst:"admin.example.com" AND src_ip:"1.2.3.0/24"
    es-search.pl --size=100 dst:"admin.example.com" AND src_ip:"1.2.0/16"

    # Show the top src_ip for 'www.example.com'
    es-search.pl --base access dst:www.example.com --top src_ip

    # Tail the access log for www.example.com 404's
    es-search.pl --base access --tail --show src_ip,file,referer_domain dst:www.example.com AND crit:404

=head1 Extended Syntax

=from_other App::ElasticSearch::Utilities::QueryString / Extended Syntax / all

=head1 Meta-Queries

Helpful in building queries is the --bases and --fields options which lists the index bases and fields:

    es-search.pl --bases

    es-search.pl --fields

    es-search.pl --base access --fields
