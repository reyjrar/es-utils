#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities::Aggregations;
use CLI::Helpers qw(:output);
use Data::Dumper;
use Test::More;

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;

# Aggregation String  Parser Testing
my %tests = (
    '00-terms' => [
        'src_ip',
        {
            'src_ip' => {
                terms => {
                    field => 'src_ip',
                    size => 20,
                }
            }
        },
    ],
    '01-terms-size' => [
        'src_ip:13',
        {
            'src_ip' => {
                terms => {
                    field => 'src_ip',
                    size => 13,
                }
            }
        },
    ],
    '02-terms-size-long' => [
        'src_ip:size=17',
        {
            'src_ip' => {
                terms => {
                    field => 'src_ip',
                    size => 17,
                }
            }
        },
    ],
    '03-terms-size-long-alias' => [
        'ips=src_ip:size=16',
        {
            'ips' => {
                terms => {
                    field => 'src_ip',
                    size => 16,
                }
            }
        },
    ],
);

foreach my $t (sort keys %tests) {
    my $agg = expand_aggregate_string( $tests{$t}->[0] );

    is_deeply( $agg, $tests{$t}->[1], $t )
        or diag( Dumper $agg );
}
done_testing();
