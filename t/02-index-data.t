#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_index_bases es_index_days_old es_index_strip_date);
use CLI::Helpers qw(:output);
use DateTime;
use Data::Dumper;
use POSIX qw(strftime);
use Test::More;

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;

my $now = DateTime->now();
my @days_old = qw(0 1 3 5 8 13 21 90);

my %TESTS=(
    'notadate-100000' => {
            es_index_bases      => 'notadate',
            es_index_days_old   => undef,
            es_index_strip_date => 'notadate-100000',
    },
    strftime('mystery-science-theater-3000-%Y.%m.%d', localtime) => {
            es_index_bases      => 'mystery,mystery-science,mystery-science-theater',
            es_index_days_old   => 0,
            es_index_strip_date => 'mystery-science-theater-3000',
    },
);
foreach my $days_old ( @days_old ) {
    # Query String Parser Testing
    my $lt = $now->clone->subtract( days => $days_old );
    my $date = $lt->strftime('%Y.%m.%d');
    my %tests = (
        "type-$date" => {
            es_index_bases      => 'type',
            es_index_days_old   => $days_old,
            es_index_strip_date => 'type',
        },
        "type-dcid-$date" => {
            es_index_bases      => 'type,type-dcid',
            es_index_days_old   => $days_old,
            es_index_strip_date => 'type-dcid',
        },
        "type_dcid_$date" => {
            es_index_bases      => 'type,type_dcid',
            es_index_days_old   => $days_old,
            es_index_strip_date => 'type_dcid',
        },
        "type_dcid_$date-0001" => {
            es_index_bases      => 'type,type_dcid',
            es_index_days_old   => $days_old,
            es_index_strip_date => 'type_dcid',
        },
    );
    # Install the test globally
    foreach my $t (keys %tests) {
        $TESTS{$t} = $tests{$t};
    }
}


foreach my $t (sort keys %TESTS) {
    my $failed = 0;
    my $got = {
        es_index_bases      => join(',', es_index_bases($t)),
        es_index_strip_date => es_index_strip_date($t),
        es_index_days_old   => es_index_days_old($t) // undef,
    };
    is_deeply($got,$TESTS{$t},sprintf "%s - %s", $t, join(',', sort keys %{$got})) or diag( Dumper $got );
}
done_testing();
