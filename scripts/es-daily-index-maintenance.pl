#!/usr/bin/env perl
# PODNAME: es-daily-index-maintenance.pl
# ABSTRACT: Run to prune old indexes and optimize existing
use strict;
use warnings;

use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
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
    'replicas',
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
my @AGES = grep { my $x = int($_); $x > 0; } split /,/, $CFG{'replicas-age'};

# Retrieve a list of indexes
my @indices = es_indices(
    check_state => 0,
    check_dates => 0,
);

# Loop through the indices and take appropriate actions;
foreach my $index (sort @indices) {
    verbose({level=>2},"$index being evaluated");

    my $days_old = es_index_days_old( $index );

    # Delete the Index if it's too old
    if( $CFG{delete} && $CFG{'delete-days'} < $days_old ) {
        output({color=>"red"}, "$index will be deleted.");
        my $rc = es_delete_index($index);
        next;
    }

    # Manage replicas
    if( $CFG{replicas} ) {
        my %shards = es_index_shards($index);
        debug({color=>"cyan"}, "$index: index is $days_old days old");
        # Default for replicas is primaries - 1;
        my $replicas = $shards{primaries} - 1;
        foreach my $age ( @AGES ) {
            if ( $days_old >= $age ) {
                $replicas--;
            }
        }
        $replicas = $CFG{'replicas-min'} if $replicas < $CFG{'replicas-min'};
        if ( $shards{primaries} > 0 && $shards{replicas} != $replicas ) {
            verbose({color=>'yellow'}, "$index: should have $replicas replicas, has $shards{replicas}");
            my $result = es_request('_settings',
                { index => $index, method => 'PUT' },
                { index => { number_of_replicas => $replicas} },
            );
            if(!defined $result) {
                output({color=>'red',indent=>1}, "Error encountered.");
            }
            else {
                output({color=>"green"}, "$index: Successfully set replicas to $replicas");
            }
        }
    }

    # Run optimize?
    if( $CFG{optimize} ) {
        my $segdata = es_segment_stats( $index );

        my $segment_ratio = undef;
        if( defined $segdata && $segdata->{shards} > 0 ) {
            $segment_ratio = sprintf( "%0.2f", $segdata->{segments} / $segdata->{shards} );
        }

        if( $days_old >= $CFG{'optimize-days'} && defined($segment_ratio) && $segment_ratio > 1 ) {
            verbose({color=>"yellow"}, "$index: required (segment_ratio: $segment_ratio).");

            my $o_res = es_optimize_index($index);
            if( !defined $o_res ) {
                output({color=>"red"}, "$index: Encountered error during optimize");
            }
            else {
                output({color=>"green"}, "$index: $o_res->{_shards}{successful} of $o_res->{_shards}{total} shards optimized.");
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
