#!/usr/bin/env perl
# PODNAME: es-daily-index-maintenance.pl
# ABSTRACT: Run to prune old indexes and optimize existing
use strict;
use warnings;

BEGIN {
    # Clear out any proxy settings
    delete $ENV{$_} for qw(http_proxy HTTP_PROXY);
}

use DateTime;
use ElasticSearch;
use JSON;
use LWP::Simple;
use Getopt::Long;
use Pod::Usage;
use App::ElasticSearch::Utilities qw(:all);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'local',
    'all',
    'host:s',
    'port:i',
    'delete',
    'delete-days:i',
    'replicas',
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

#------------------------------------------------------------------------#
# Host or Local
pod2usage(-exitval => 1, -message => "Destination host not specified, use --local or --host.") unless defined $opt{local} or defined $opt{host};

my %CFG = (
    'optimize-days'  => 1,
    'delete-days'    => 90,
    'replicas-age'   => "1,30",
    'index-basename' => 'logstash',
    'date-separator' => '.',
    timezone         => 'Europe/Amsterdam',
    port             => 9200,
    delete           => 0,
    optimize         => 0,
    replicas         => 0,
);
my @MODES = qw(delete optimize replicas);

# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    $CFG{$setting} = $opt{$setting} if exists $opt{$setting} and defined $opt{$setting};
}
if ( exists $opt{all} && $opt{all} ) {
    $CFG{$_} = 1 for @MODES;
}
else {
    my $operate = 0;
    foreach my $mode (@MODES) {
        $operate++ if $CFG{$mode};
        last if $operate;
    }
    pod2usage(-message => "No operation selected, use --delete, --optimize, or --replicas.", -exitval => 1) unless $operate;
}

# Create the target uri for the ES Cluster
my $TARGET = exists $opt{host} && defined $opt{host} ? $opt{host} : 'localhost';
$TARGET .= ":$CFG{port}";
debug("Target is: $TARGET");
debug_var(\%CFG);

my $es = ElasticSearch->new(
    servers   => [ $TARGET ],
    transport => 'http',
    timeout   => 0,     # Do Not Timeout
);

# Delete Indexes older than a certain point
my $DEL      = DateTime->now(time_zone => $CFG{timezone})->truncate( to => 'day' )->subtract( days => $CFG{'delete-days'} );
my $OPTIMIZE = DateTime->now(time_zone => $CFG{timezone})->truncate( to => 'day' )->subtract( days => $CFG{'optimize-days'} );
my @AGES = grep { my $x = int($_); $x > 0 } split /,/, $CFG{'replicas-age'};

# Retrieve a list of indexes
my $d_res = $es->cluster_state(
    filter_nodes         => 1,
    filter_routing_table => 1,
);
my $indices = $d_res->{metadata}{indices};
if ( !defined $indices ) {
    output({color=>"red"}, "Unable to locate indices in status!");
    exit 1;
}

# Loop through the indices and take appropriate actions;
foreach my $index (sort keys %{ $indices }) {
    verbose("$index being evaluated");
    debug_var($indices->{$index});

    my @words = split /\-/, $index;
    my $dateStr = pop @words;

    next unless defined $dateStr && $dateStr =~ /\d{4}.?\d{2}.?\d{2}/;

    my $basename = join('-', @words);
    debug("Basename: $basename");
    debug("Date string: $dateStr");
    my %words = map { $_=>1 } @words;
    next unless exists $words{$CFG{'index-basename'}};

    my $sep = $CFG{'date-separator'};
    my @parts = split /\Q$sep\E/, $dateStr;
    my $idx_dt = DateTime->new( year => $parts[0], month => $parts[1], day => $parts[2] );

    # Delete the Index if it's too old
    if( $CFG{delete} && $idx_dt < $DEL ) {
        output({color=>"red"}, "$index will be deleted.");
        eval {
            my $rc = $es->delete_index( index => $index );
        };
        next;
    }

    # Manage replicas
    if( $CFG{replicas} ) {
        my $r_dt = $idx_dt->clone();
    }

    # Run optimize?
    if( $CFG{optimize} ) {
        my $segment_ratio = undef;
        eval {
            my $json = get( qq{http://$TARGET/$index/_segments} );
            my $res = decode_json( $json );
            my $shard_data = $res->{indices}{$index}{shards};
            my $shards = 0;
            my $segments = 0;
            foreach my $id (keys %{$shard_data} ){
                $segments += $shard_data->{$id}[0]{num_search_segments};
                $shards++;
            }
            if( $shards > 0 ) {
                $segment_ratio = sprintf( "%0.2f", $segments / $shards );
            }
        };

        if( $idx_dt <= $OPTIMIZE && defined($segment_ratio) && $segment_ratio > 1 ) {
            my $error = undef;
            verbose({color=>"yellow"}, "$index: required (segment_ratio: $segment_ratio).");

            eval {
                my $o_res = $es->optimize_index(
                    index            => $index,
                    max_num_segments => 1,
                    wait_for_merge   => 0,
                );
                output({color=>"green"}, "$index: $o_res->{_shards}{successful} of $o_res->{_shards}{total} shards optimized.");
            };
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
    --local             Poll localhost and use name reported by ES
    --host|-H           Host to poll for statistics
    --local             Assume localhost as the host
    --all               Run delete and optimize
    --delete            Run delete indexes older than
    --delete-days       Age of oldest index to keep (default: 90)
    --optimize          Run optimize on indexes
    --optimize-days     Age of first index to optimize (default: 1)
    --replicas          Manage replicas
    --replicas-age      CSV list of ages in days to decrement number of replicas
    --index-basename    Default is 'logstash'
    --date-separator    Default is '.'
    --quiet             Ideal for running on cron, only outputs errors
    --verbose           Send additional messages to STDERR

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

Run the replicas hook

=item B<replicas-age>

Comma separated list of ages to decrement the number of replicas examples:

    --replicas-age 30

Decrement replicas for every 30 days of age of the index

    --replicas-age 1,30

Decrement replicas after 1 day, then every 30 days thereafter

    --replicas-age 1,30,90

Decrement repliacs after 1 day, then after 30 days, and then at 90 days and every 90 days thereafter


=item B<local>

Optional, operate on localhost (if not specified, --host required)

=item B<host>

Optional, the host to maintain (if not specified --local required)

=item B<verbose>

Verbose stats, to not interfere with cacti, output goes to STDERR

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit


=back

=head1 DESCRIPTION

This script assists in maintaining the indexes for logging clusters through
routine deletion and optimization of indexes.

Use with cron:

    22 4 * * * es-daily-index-maintenance.pl --local --all --delete-days=180

=cut
