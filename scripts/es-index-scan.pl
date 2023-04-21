#!perl
# PODNAME: es-index-scan.pl
# ABSTRACT: Scan indexes for potential issues
use v5.16;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CLI::Helpers qw(:output);

my $indexes = es_indices(check_dates => 0);

foreach my $idx ( sort @{ $indexes } ) {
    my $age = es_index_days_old( $idx );
    my $result = es_request("/$idx/_stats");
    my $stats = $result->{indices}{$idx}{primaries};

    my $doc_size = es_human_count($stats->{docs}{count} || 0);
    my $size = es_human_size($stats->{store}{size_in_bytes});
    output("checking $idx.. (age=${age}d, docs=$doc_size, size=$size)");

    my $segments = $stats->{segments}{count};
    my $shards   = $stats->{shard_stats}{total_count};

    if( my $docs = $stats->{docs}{count} ) {
        my $deleted = $stats->{docs}{deleted};
        output({indent=>1,color=>'blue'}, sprintf "%d of %d (%0.1f%%) documents deleted",
            $deleted, $docs, ($deleted / $docs) * 100,
        ) if $deleted;
    }
    else {
        output({indent=>1,color=>'red'}, "no documents")
    }

    output({indent=>1,color=>'red'}, "More than one segment per shard: segments=$segments, shards=$shards")
        if $age > 1 && $segments > $shards;


    output({indent=>1,color=>'magenta'}, "index never queried")
        unless $stats->{search}{query_total};

    output({indent=>1,color=>'yellow'}, "$stats->{indexing}{index_failed} index failures")
        if $stats->{indexing}{index_failed};

    output({indent=>1,color=>'red'}, "indexing throttled")
        if $stats->{indexing}{is_throttled};

    output({indent=>1,color=>'yellow'}, sprintf "%0.3fs of throttled indexing", $stats->{indexing}{throttle_time_in_millis} / 1000)
        if $stats->{indexing}{throttle_time_in_millis};
}

