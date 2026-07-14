#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities::VersionHacks qw(_fix_version_request);
use Data::Dumper;
use Test::More;
use Storable qw(dclone);

my @TESTS = load_tests();

foreach my $t (@TESTS) {
    foreach my $ver (sort keys %{ $t->{expected} }) {
        my @got = _fix_version_request($ver, @{ dclone $t->{input} });
        is_deeply( \@got, $t->{expected}{$ver}, sprintf "version tweak - %s - %s", $t->{name}, $ver )
            or diag( Dumper \@got );
    }
}
done_testing;

sub load_tests {
    return(
        {
            name => '_status',
            input => [
                '_status',
                undef,
                undef,
            ],
            expected => {
                '7.12' => [
                    '_stats',
                    undef,
                    undef,
                ],
            },
        },
        {
            name => '_optimize',
            input => [
                '_optimize',
                undef,
                undef,
            ],
            expected => {
                '5.8' => [
                    '_forcemerge',
                    undef,
                    undef,
                ],
                '7.12' => [
                    '_forcemerge',
                    undef,
                    undef,
                ],
                '8.19' => [
                    '_forcemerge',
                    undef,
                    undef,
                ],
            },
        },
        {
            name => '_cluster/nodes',
            input => [
                '_cluster/nodes',
                undef,
                undef,
            ],
            expected => {
                '7.12' => [
                    '_nodes',
                    undef,
                    undef,
                ],
            },
        },
        {
            name => '_cat/shards',
            input => [
                '_cat/shards',
                {uri_param => {local => 1}},
                undef,
            ],
            expected => {
                '7.10' => [
                    '_cat/shards',
                    {uri_param => {local => 1}},
                    undef,
                ],
                '7.12' => [
                    '_cat/shards',
                    {uri_param => {}},
                    undef,
                ],
            },
        },
        {
            name => 'search_params',
            input => [
                '_search',
                {uri_param => {
                    rest_total_hits_as_int => 1,
                    track_total_hits => 1,
                }},
                {},
            ],
            expected => {
                '5.8' => [
                    '_search',
                    {uri_param => {}},
                    {},
                ],
                '6.8' => [
                    '_search',
                    {uri_param => {track_total_hits => 1}},
                    {},
                ],
            },
        },
        {
            name => '_cluster/state no parameters',
            input => [
                '_cluster/state',
                {},
                undef,
            ],
            expected => {
                '7.10' => [
                    '_cluster/state/_all',
                    {},
                    undef,
                ],
            },
        },
        {
            name => '_cluster/state filters',
            input => [
                '_cluster/state',
                {uri_param => {filter_nodes=>1}},
                undef,
            ],
            expected => {
                '7.10' => [
                    '_cluster/state/blocks,indices,master_node,metadata,routing_table,version',
                    {uri_param => {}},
                    undef,
                ],
            },
        },
    );
}
