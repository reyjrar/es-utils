#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_flatten_hash);
use Test::More;

my @tests = (
    {
        case => { a => 1, b => { c => 2, d => 3 } },
        expected => {qw(
            a 1
            b.c 2
            b.d 3
        )}
    },
    {
        case => {qw(
            a.b.c 3
            d.e.f.g 4
            h 5
        )},
        expected => {qw(
            a.b.c 3
            d.e.f.g 4
            h 5
        )},
    }
);

foreach my $test (@tests) {
    my $got = es_flatten_hash( $test->{case} );
    is_deeply($got, $test->{expected});
}

done_testing();
