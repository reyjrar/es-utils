#!perl
# PODNAME: es-index-optimize.pl
# ABSTRACT: Force merge indexes safely
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CLI::Helpers qw(:all);
use Const::Fast;
use JSON::MaybeXS;
use Getopt::Long::Descriptive;
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
const my %DEFAULT => (
    batch_size       => 10,
    max_num_segments => 1,
);
my ($opt,$usage) = describe_options('%c %o',
    ['min-docs|m=i', "Minimum documents in an index to optimize"],
    ['max-docs|M=i', "Maximum documents in an index to optimize"],
    ['batch-size|b=i', "Number of indices optimize at a time, defaults to $DEFAULT{batch_size}, set to 0 to do everything",
        { default => $DEFAULT{batch_size} }
    ],
    ['max-num-segments|n=i', "Maximum number of segments per shard, defaults to $DEFAULT{max_num_segments}",
        { default => $DEFAULT{max_num_segments} }
    ],
    ['force', "Force the optimization, even if we are at the correct number of segments"],
    []     ,
    ['help', 'Display this message', { shortcircuit => 1 }],
    ['manual', 'Display full manual', { shortcircuit => 1 }],
);

#------------------------------------------------------------------------#
# Documentations!
if( $opt->help ) {
    print $usage->text;
    exit 0;
}
pod2usage(-exitstatus => 0, -verbose => 2) if $opt->manual;

#------------------------------------------------------------------------#
my $json = JSON->new->pretty->utf8->canonical;

my %indices = map { $_ => (es_index_days_old($_) || 0) } es_indices();

my $optimizing = 0;
foreach my $idx ( sort { $indices{$b} <=> $indices{$a} } keys %indices ) {
    if( $indices{$idx} < 1 ) {
        output({color=>'magenta'}, "Skipping today's index $idx");
        next;
    }

    verbose({color=>'cyan'}, "Evaluating $idx");

    if( $opt->min_docs or $opt->max_docs ) {
        if( my $docs = es_index_docs($idx) ) {
            if( $opt->min_docs && $docs < $opt->min_docs ) {
                output({color=>'yellow'},
                    sprintf "%s skipped with %d docs, less than the minimum",
                        $idx,
                        $docs,
                );
                next;
            }
            if( $opt->max_docs && $docs > $opt->max_docs ) {
                output({color=>'yellow'},
                    sprintf "%s skipped with %d docs, more than the maximum",
                        $idx,
                        $docs,
                );
                next;
            }
            verbose({indent=>1}, "$idx passes document count check with $docs docs");
        }
    }

    next unless $opt->force || needs_optimizing($idx);

    next if already_optimizing($idx);

    output(sprintf "%s optimizing%s", $idx, $opt->force ? ' [forced]' : '');

    if( ++$optimizing >= $opt->batch_size ) {
        last unless confirm("Currently optimizing $optimizing indices, continue?");
    }

    optimize_index($idx);
}

sub needs_optimizing {
    my ($index) = @_;

    my $segment_ratio = undef;
    my $segdata = es_segment_stats( $index );
    if( defined $segdata && $segdata->{shards} > 0 ) {
        $segment_ratio = sprintf( "%0.2f", $segdata->{segments} / $segdata->{shards} );
    }

    die "Failed to get segment stats for $index, bailing"
        unless defined $segment_ratio;

    if( $segment_ratio > $opt->max_num_segments ) {
        verbose({color=>"yellow", indent => 1}, "$index: required (segment_ratio: $segment_ratio).");

        return 1;

    }
    else {
        verbose({indent=>1},"$index already optimized");
    }

    return;
}

sub already_optimizing {
    my ($index) = @_;

    my %options = (
        uri_param => {
            actions => "*merge*",
            detailed => 'true',
        }
    );
    if ( my $res = es_request('_cat/tasks', \%options) ) {
        my $desc = "Force-merge indices [$index]";
        foreach my $task ( @{ $res } ) {
            if ( index($task->{description}, $desc) >= 0 ) {
                output({color=>'green'}, "$index skipped, already merging");
                return 1;
            }
        }
    }
    return;
}

sub optimize_index {
    my ($index) = @_;
    my $result = es_optimize_index($index,  wait_for_completion => 'false' );
    if( !defined $result ) {
        output({color=>"red",indent => 1}, " !! Encountered error during optimize !!");
    }
    else {
        use DDP;
        p( $result );
        output({color=>"green",indent => 1}, "= $result->{_shards}{successful} of $result->{_shards}{total} shards optimized.");
    }
}
