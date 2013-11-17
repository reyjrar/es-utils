#!/usr/bin/env perl
# PODNAME: es-daily-index-maintenance.pl
# ABSTRACT: Run to prune old indexes and optimize existing
use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;

use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(:all);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'all',
    'dry-run',
    'delete',
    'delete-days:i',
    'replicas:i',
    'replicas-min:i',
    'replicas-age:s',
    'optimize',
    'optimize-days:i',
    'index-basename:s',
    'date-separator:s',
    'timezone:s',
    # Basic options
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(-exitval => 0) if $opt{help};
pod2usage(-exitval => 0, -verbose => 2) if $opt{manual};

my %CFG = (
    'optimize-days'  => 1,
    'delete-days'    => 90,
    'replicas-min'   => 0,
    'dry-run'        => 0,
    'replicas-age'   => "1,30",
    timezone         => 'Europe/Amsterdam',
    delete           => 0,
    optimize         => 0,
    replicas         => 0,
);
# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    $CFG{$setting} = $opt{$setting} if exists $opt{$setting} and defined $opt{$setting};
}
# Turn on verbose if debug is enabled
override(verbose => 1) if $CFG{'dry-run'};

# Figure out what to run
my @MODES = qw(delete optimize replicas);
if ( exists $opt{all} && $opt{all} ) {
    map {
        $CFG{$_} = 1 unless $_ eq 'replicas';
    } @MODES;
}
else {
    my $operate = 0;
    foreach my $mode (@MODES) {
        $operate++ if $CFG{$mode};
        last if $operate;
    }
    pod2usage(-message => "No operation selected, use --delete, --optimize, or --replicas.", -exitval => 1) unless $operate;
}
# Can't have replicas-min below 0
$CFG{'replicas-min'} = 0 if $CFG{'replicas-min'} < 0;

# Create the target uri for the ES Cluster
my $es = es_connect();

# Ages for replica management
my @AGES     = grep { my $x = int($_); $x > 0; } split /,/, $CFG{'replicas-age'};

# Retrieve a list of indexes
my @indices = es_indices();

# Loop through the indices and take appropriate actions;
foreach my $index (sort @indices) {
    verbose("$index being evaluated");

    my $days_old = es_index_days_old( $index );

    # Delete the Index if it's too old
    if( $CFG{delete} && $CFG{'delete-days'} < $days_old ) {
        output({color=>"red"}, "$index will be deleted.");
        eval {
            my $rc = $es->delete_index( index => $index );
        } unless $CFG{'dry-run'};
        next;
    }

    # Manage replicas
    if( $CFG{replicas} ) {
        my $current_replicas = es_index_shard_replicas($index);
        debug({color=>"cyan"}, " - index is $days_old days old");
        my $replicas = $CFG{replicas};
        foreach my $age ( @AGES ) {
            if ( $days_old >= $age ) {
                $replicas--;
            }
        }
        $replicas = $CFG{'replicas-min'} if $replicas < $CFG{'replicas-min'};
        if ( defined $current_replicas && $replicas != $current_replicas ) {
            verbose({color=>'yellow'}, " - should have $replicas replicas, has $current_replicas");
            eval {
                $es->update_index_settings(
                    index => $index,
                    settings => { number_of_replicas => $replicas},
                );
            } unless $CFG{'dry-run'};
            if (my $err = $@ ) {
                output({color=>'red'}, "Failed setting replicas to $replicas for $index.", $err);
            }
        }
    }

    # Run optimize?
    if( $CFG{optimize} ) {
        my $segdata = es_index_segments( $index );

        my $segment_ratio = undef;
        if( defined $segdata && $segdata->{shards} > 0 ) {
            $segment_ratio = sprintf( "%0.2f", $segments / $shards );
        }

        if( $days_old >= $CFG{'optimize-days'} && defined($segment_ratio) && $segment_ratio > 1 ) {
            my $error = undef;
            verbose({color=>"yellow"}, "$index: required (segment_ratio: $segment_ratio).");

            eval {
                my $o_res = $es->optimize_index(
                    index            => $index,
                    max_num_segments => 1,
                    wait_for_merge   => 0,
                );
                output({color=>"green"}, "$index: $o_res->{_shards}{successful} of $o_res->{_shards}{total} shards optimized.");
            } unless $CFG{'dry-run'};
            if( my $error = $@ ) {
                output({color=>"red"}, "$index: Encountered error during optimize: $error");
            }
        }
        else {
            if( defined($segment_ratio) && $segment_ratio > 1 ) {
                verbose("$index is active not optimizing (segment_ratio:$segment_ratio)");
            }
            else {
                verbose("$index already optimized");
            }
        }
    }
}

__END__

=head1 SYNOPSIS

es-daily-index-maintenance.pl --all --local

Options:

    --help              print help
    --manual            print full manual
    --dry-run           Tell me what you would do, but don't do it.
    --all               Run delete and optimize
    --delete            Run delete indexes older than
    --delete-days       Age of oldest index to keep (default: 90)
    --optimize          Run optimize on indexes
    --optimize-days     Age of first index to optimize (default: 1)
    --replicas          Sets the number of initial replicas, and manages replica aging
    --replicas-age      CSV list of ages in days to decrement number of replicas
    --replicas-min      Minimum number of replicas this index may have, default:0

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<optimize>

Run the optimization hook

=item B<optimize-days>

Integer, optimize indexes older than this number of days

=item B<delete>

Run the delete hook

=item B<delete-days>

Integer, delete indexes older than this number of days

=item B<replicas>

Sets the number of initial replicas for an index type.  This is used to compute the
expected number of replicas based on the the age of the index.

    --replicas=2

=item B<replicas-age>

A comma separated list of the ages at which to decrement to the number of replicas, the default is:

    --replicas-age 1,30

Can be as long as you'd like, but the replica aging will stop at the replicas-min.

=item B<replicas-min>

The minimum number of replicas to allow replica aging to set.  The default is 0

    --replicas-min=1

=back

=head1 DESCRIPTION

This script assists in maintaining the indexes for logging clusters through
routine deletion and optimization of indexes.

Use with cron:

    22 4 * * * es-daily-index-maintenance.pl --local --all --delete-days=180

=cut
