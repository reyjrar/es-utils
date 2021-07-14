#!perl
#
use v5.10;
use warnings;

use App::ElasticSearch::Utilities qw(es_request);
use App::ElasticSearch::Utilities::QueryString;
use App::ElasticSearch::Utilities::Aggregations;
use Getopt::Long::Descriptive;
use DDP;
use YAML ();

# Grab a copy of the args
my @args = @ARGV;
# Process args
my ($opt,$usage) = describe_options("%c %o",
    ['aggregate|agg=s@', "Aggregate these fields, specified more than once to sub aggregate", { required => 1 }],
    ['by=s@',            "Sort by this aggregation" ],
    ['asc',              "Sort ascending, default is descnding" ],
    [],
    ['help', "Display this help", { shortcircuit => 1 } ],
);
if( $opt->help ) {
    print $usage->text;
    exit 0;
}

my $qs = App::ElasticSearch::Utilities::QueryString->new();
my $q  = $qs->expand_query_string( @ARGV );

# Figure out where the --by's are spatially
my $ORDER     = $opt->asc ? 'asc' : 'desc';
my @agg_param = @{ $opt->aggregate };
my @by_param  = @{ $opt->by };

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

print YAML::Dump($q->aggregations);
