#!perl
#
use v5.10;
use warnings;

use App::ElasticSearch::Utilities qw(es_request);
use App::ElasticSearch::Utilities::QueryString;
use Getopt::Long::Descriptive;
use DDP;

my ($opt,$usage) = describe_options("%c %o",
    ['aggregate|agg=s@', "Aggregate these fields, specified more than once to sub aggregate", { required => 1 }],
    ['by=s',             "Sort by this aggregation" ],
    ['asc',              "Sort ascending, default is descnding" ],
    [],
    ['help', "Display this help", { shortcircuit => 1 } ],
);
if( $opt->help ) {
    print $usage->text;
    exit 0;
}

my %Handlers = (
    terms => {
        params => sub { { size => $_[0] } },
    },
    significant_terms => {
        params => sub { { size => $_[0] } },
    },
    cardinality => {
        single_stat => 1,
    },
    avg => {
        single_stat => 1,
    },
    weighted_avg => {},
    extend_stats => {},
    stats => {},
    min => { single_stat => 1 },
    max => { single_stat => 1 },
    sum => { single_stat => 1 },
    histogram => {
        params => sub {
            return unless $_[0] > 0;
            return { interval => $_[0] };
        },
    },
    percentiles => {
        params => sub {
            my @pcts = $_[0] ? split /,/, $_[0] : qw(25 50 75 90);
            return { percents => \@pcts };
        },
    },

);

my $qs = App::ElasticSearch::Utilities::QueryString->new();
my $q  = $qs->expand_query_string( @ARGV );

my @aggs = reverse @{ $opt->aggregate };
my $base = es_expand_simple_aggregate(shift @aggs);
$q->add_aggs( %{ $base } );
foreach my $def ( @aggs ) {
    $q->wrap_aggs( %{ es_expand_simple_aggregate($def) } );
}


p($q->aggregations);

sub es_expand_simple_aggregate {
    my ($token) = @_;

    my %aggs = ();
    foreach my $def ( split ';', $token ) {
        my $alias = $def =~ s/\=([^=]+)$// ? $1 : undef;
        my @parts = split /:/, $def, 3;
        if( @parts == 1 ) {
            $aggs{$def} = { terms => { field => $def, size => 20 } };
            next;
        }
        my ($agg, $field);
        if( exists $Handlers{$parts[0]} ) {
            $agg     = shift @parts;
            $field   = shift @parts;
        }
        else {
            $agg = 'terms';
            $field = shift @parts;
        }
        my $params  = {};
        if( exists $Handlers{$agg}->{params} ) {
            # Process parameters
            $params = $Handlers{$agg}->{params}->(@parts);
        }
        $alias ||= join "_", $agg eq 'terms' ? ($field) : ($agg, $field);
        $aggs{$alias} = { $agg => { field => $field, %{ $params } } };
    }
    return \%aggs;
}
