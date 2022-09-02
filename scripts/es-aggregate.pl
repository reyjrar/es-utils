#!perl
#
# PODNAME: es-aggregate.pl
# ABSTRACT: Multi-level aggregations in Elasticsearch
#
use v5.10;
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_request);
use App::ElasticSearch::Utilities::QueryString;
use App::ElasticSearch::Utilities::Aggregations;
use CLI::Helpers qw(:output);
use Getopt::Long::Descriptive;
use JSON::MaybeXS;
use Pod::Usage;
use Storable qw(dclone);
use YAML::XS ();

# Grab a copy of the args
my @args = @ARGV;
# Process args
my ($opt,$usage) = describe_options("%c %o",
    ['aggregate|agg=s@', "Aggregate these fields, specified more than once to sub aggregate", { required => 1 }],
    ['by=s@',            "Sort by this aggregation" ],
    ['asc',              "Sort ascending, default is descnding" ],
    [],
    ["Display"],
    ['json',      "Results as JSON"],
    ['show-aggs', "Show computed aggregation block"],
    ['show-raw',  "Show raw results from Elasticsearch"],
    [],
    ['help', "Display this help", { shortcircuit => 1 } ],
    ['manual', "Display complete options and documentation.", { shortcircuit => 1 }],
);
if( $opt->help ) {
    print $usage->text;
    exit 0;
}
pod2usage(-exitval => 0, -verbose => 2) if $opt->manual;

my $json = JSON->new->utf8->canonical;
my $qs = App::ElasticSearch::Utilities::QueryString->new();
my $q  = $qs->expand_query_string( @ARGV );
$q->set_size(0);

# Figure out where the --by's are spatially
my $ORDER     = $opt->asc ? 'asc' : 'desc';
my @agg_param = @{ $opt->aggregate };
my @by_param  = $opt->by ? @{ $opt->by } : ();
my @by        = ();

foreach my $token ( reverse @args ) {
    if( $token =~ /^--agg/ ) {
        $q->wrap_aggs( %{ expand_aggregate_string( pop @agg_param ) } );
        $q->aggs_by( $ORDER => [@by] ) if @by;
        @by=();
    }
    elsif( $token eq '--by' ) {
        push @by, pop @by_param;
    }
}

output({color=>'yellow'}, YAML::XS::Dump($q->aggregations)) if $opt->show_aggs;

my $result = $q->execute();
my $aggs   = $result->{aggregations};

output({color=>'cyan'}, YAML::XS::Dump($aggs)) if $opt->show_raw;

my $flat = es_flatten_aggs($aggs);
foreach my $row ( @{ $flat } ) {
    if ( $opt->json ) {
        output({data=>1}, $json->encode({ @{ $row } }));
    }
    else {
        output({data=>1}, join("\t", grep { !/\.hits$/  } @{ $row }));
    }
}
__END__

=head1 SYNOPSIS

es-aggregate.pl [search string] --agg <aggregate>

Options:

    --agg               Aggregation string, can be specified multiple times
    --by                Perform an aggregation using the result of this, example: --by cardinality:src_ip
    --asc               Change default sort order to ascending
    --show-agg          Show the aggregate clause being sent to the backend
    --show-raw          Show the raw results from the backend
    --json              Output as newline delimited JSON

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=back
