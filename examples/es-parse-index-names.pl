#!/usr/bin/env perl

use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CLI::Helpers qw(:output);

my @index = @ARGV ? @ARGV : es_indices();

foreach my $idx ( @index ) {
    output(
        sprintf "%s : stripped=%s age=%d bases=%s",
            $idx,
            es_index_strip_date($idx),
            es_index_days_old($idx),
            join(',', es_index_bases($idx)),
    );
}
