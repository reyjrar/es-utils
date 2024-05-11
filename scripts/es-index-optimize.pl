#!perl
# PODNAME: es-index-optimize.pl
# ABSTRACT: Force merge indexes safely
use v5.16;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CHI;
use CLI::Helpers qw(:all);
use Const::Fast;
use JSON::MaybeXS;
use Getopt::Long::Descriptive;
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
const my %DEFAULT => (
);
my ($opt,$usage) = describe_options('%c %o',
    ['min-docs|m=i', "Minimum documents in an index to optimize"],
    ['max-docs|M=i', "Maximum documents in an index to optimize"],
    ['max-concurrent-merges|C=i', "Maximum concurrent merge jobs defaults to 2/3 of number of data nodes" ],
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
my $cache = CHI->new(
    driver     => 'Memory',
    global     => 1,
    expires_in => '1m',
);

my %indices = map { $_ => (es_index_days_old($_) || 0) } es_indices();

my $optimized = 0;
foreach my $idx ( sort { $indices{$b} <=> $indices{$a} || $a cmp $b } keys %indices ) {
    # Always skip the current days index
    if( $indices{$idx} < 1 ) {
        output({color=>'magenta'}, "Skipping today's index $idx");
        next;
    }

    verbose({color=>'cyan'}, "Evaluating $idx");

    # Optionally check the number of documents
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

    # Ensure it needs optimizing
    next unless $opt->force || needs_optimizing($idx);

    # Check for existing optimizing runs
    next if already_optimizing($idx);

    # wait for other merges to finish
    check_merges();

    output(sprintf "%s optimizing%s", $idx, $opt->force ? ' [forced]' : '');
    optimize_index($idx);
    $optimized++;
}

output({color=>'green'}, "Optimized $optimized indices");

sub needs_optimizing {
    my ($index) = @_;

    my $segment_ratio = undef;
    my $segdata = es_segment_stats( $index );
    if( defined $segdata && $segdata->{shards} > 0 ) {
        $segment_ratio = sprintf( "%0.2f", $segdata->{segments} / $segdata->{shards} );
        debug({color=>"cyan", indent => 1}, "$index: $segdata->{segments} segs, $segdata->{shards} shards (segment_ratio: $segment_ratio).");
    }

    die "Failed to get segment stats for $index, bailing"
        unless defined $segment_ratio;

    if( $segdata->{segments} > $segdata->{shards}  ) {
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

    if ( my $res = _get_merge_tasks() ) {
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

sub check_merges {
    my $pause = 5;

    while( 1 ) {
        my $res = _get_merge_tasks();

        die "Unable to check merge tasks in process, exitting"
            unless defined $res;

        my $current_merges = @{ $res };

        last if $current_merges < get_max_concurrent_merges();

        output({indent=>1, color=>'blue'}, "currently $current_merges running, waiting ${pause}s");
        sleep $pause;
    }
}

sub get_max_concurrent_merges {
    # Simplest case, specified by the user
    return $opt->max_concurrent_merges if $opt->max_concurrent_merges;

    state $merges;
    return $merges if $merges;

    # Compute the number of data nodes
    my $res = es_request('_cat/nodes');
    my $data_nodes = 0;
    foreach my $node ( @{$res} ) {
        $data_nodes++ if $node->{'node.role'} =~ /d/;
    }

    # Compute merges;
    return $merges = int( $data_nodes * (2/3) );
}

sub optimize_index {
    my ($index) = @_;
    eval {
        my $result = es_optimize_index($index,  wait_for_completion => 'false' );
        if( !defined $result ) {
            output({color=>"red",indent => 1}, " !! Encountered error during optimize !!");
        }
        else {
            output({color=>"green",indent => 1}, "= $result->{_shards}{successful} of $result->{_shards}{total} shards optimized.");
        }
        1;
    } or do {
        my $err = $@;
        output({color=>'magenta', indent => 1}, '+ forcemerge requested');
    };
}

sub _get_merge_tasks {
    my %options = (
        uri_param => {
            actions => "*merge*",
            detailed => 'true',
        }
    );

    return es_request('_cat/tasks', \%options);
}
