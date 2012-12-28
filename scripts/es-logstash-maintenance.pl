#!/usr/local/bin/booking-perl
# PODNAME: es-logstash-maintenance.pl
# ABSTRACT: Run to prune old indexes and optimize existing
use strict;
use warnings;

BEGIN {
    delete $ENV{$_} for qw(http_proxy HTTP_PROXY);
}

use DateTime;
use ElasticSearch;
use Try::Tiny;
use JSON::XS;
use LWP::Simple;
use Getopt::Long;
use Pod::Usage;

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
    'optimize',
    'optimize-days:i',
    'index-basename:s',
    'date-separator:s',
    'timezone:s',
    # Basic options
    'help|h',
    'manual|m',
    'verbose|v',
    'quiet|q',
    'debug|d',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

#------------------------------------------------------------------------#
# Host or Local
pod2usage(1) if !$opt{local} and !$opt{host};
# Must choose a Mode
pod2usage(1) if !$opt{all} and (!$opt{optimize} or !$opt{delete});

my %CFG = (
    'optimize-days'  => 1,
    'delete-days'    => 90,
    'index-basename' => 'logstash',
    'date-separator' => '.',
    timezone         => 'Europe/Amsterdam',
    port             => 9200,
);
# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    $CFG{$setting} = $opt{$setting} if exists $opt{$setting} and defined $opt{$setting};
}
my $TARGET = exists $opt{host} && defined $opt{host} ? $opt{host} : 'localhost';
$TARGET .= ":$CFG{port}";

my $es = ElasticSearch->new(
    servers   => [ $TARGET ],
    transport => 'http',
    timeout   => 0,     # Do Not Timeout
);

# Delete Indexes older than a certain point
my $DEL      = DateTime->now(time_zone => $CFG{timezone})->truncate( to => 'day' )->subtract( days => $CFG{'delete-days'} );
my $OPTIMIZE = DateTime->now(time_zone => $CFG{timezone})->truncate( to => 'day' )->subtract( days => $CFG{'optimize-days'} );
my $d_res    = $es->index_stats(
    index => undef,
    type  => undef,
    clear => 1
);
# Loop through the indices and take appropriate actions;
foreach my $index (sort keys %{ $d_res->{_all}{indices} }) {
    print "$index being evaluated\n";


    my ($basename,$dateStr) = split /\-/, $index;
    next unless $basename eq $CFG{'index-basename'};

    my @parts = split $CFG{'date-separator'}, $dateStr;
    my $idx_dt = DateTime->new( year => $parts[0], month => $parts[1], day => $parts[2] );

    # Delete the Index if it's too old
    if( $idx_dt < $DEL ) {
        print "$index will be deleted.\n";
        try {
            my $rc = $es->delete_index( index => $index );
        };
        next;
    }

    # Check if we can optimize
    my $segment_ratio = undef;
    try {
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
        print " + Optimization required (segment_ratio: $segment_ratio).\n";

        try {
            my $o_res = $es->optimize_index(
                index            => $index,
                max_num_segments => 1,
                wait_for_merge   => 0,
            );
            print " + Success: $o_res->{_shards}{successful} of $o_res->{_shards}{total} shards optimized.\n";
        } catch {
            $error = shift;
        };
        print " ! Encountered error during optimize: $error\n" if defined $error;
    }
    else {
        print " + ";
        if( $segment_ratio > 1 ) {
            print " Active index, not optimizing.";
        }
        else {
            print " Already optimized!";
        }
        print " (segment_ratio:$segment_ratio)\n";
    }
    print "\n";
}


__END__

=head1 SYNOPSIS

es-logstash-maintenance.pl --all --local

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
    --index-basename    Default is 'logstash'
    --date-separator    Default is '.'
    --quiet             Ideal for running on cron, only outputs errors
    --verbose           Send additional messages to STDERR

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<local>

Optional, operate on localhost (if not specified, --host required)

=item B<host>

Optional, the host to maintain (if not specified --local required)

=item B<verbose>

Verbose stats, to not interfere with cacti, output goes to STDERR

=back

=head1 DESCRIPTION

This script assists in maintaining the indexes for logging clusters through
routine deletion and optimization of indexes.

Use with cron:

    22 4 * * * es-logstash-maintenance.pl --local --all --delete-days=180

=head1 AUTHOR

Brad Lhotsky <brad.lhotsky@gmail.com>

=cut
