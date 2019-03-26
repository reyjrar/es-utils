#!perl
# PODNAME: es-graphite-dynamic.pl
# ABSTRACT: Dynamically gather metrics and send to graphite
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_request es_node_stats es_index_stats);
use CLI::Helpers qw(:all);
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use IO::Socket::INET;
use Pod::Usage;
use Ref::Util qw(is_hashref is_arrayref);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'ignore=s',
    'carbon-base=s',
    'carbon-proto=s',
    'carbon-server=s',
    'carbon-port=i',
    'with-indices',
    'with-cluster',
    'prefix=s',
    'no-prefix',
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

#------------------------------------------------------------------------#
# Argument Sanitazation

# Ignore uninteresting metrics
my @_IGNORE = qw(id attributes timestamp upms _all _shards);
push @_IGNORE, split(/,/, $opt{ignore}) if exists $opt{ignore};
my %_IGNORE = map { $_ => 1 } @_IGNORE;
# Merge options into config
my %cfg = (
    'carbon-proto' => 'tcp',
    'carbon-base'  => 'general.es',
    %opt,
    host => App::ElasticSearch::Utilities::def('HOST'),
);

#------------------------------------------------------------------------#
# Globals
my $TIME     = time;
my $HOSTNAME = undef;
my $CLUSTER  = undef;

#------------------------------------------------------------------------#
# Carbon Socket Creation
my $carbon_socket;
if( exists $cfg{'carbon-server'} and length $cfg{'carbon-server'} ) {
    my %valid_protos = ( tcp => 1, udp => 1 );
    die "invalid protocol specified: $cfg{'carbon-proto'}\n" unless exists $valid_protos{$cfg{'carbon-proto'}};
    $carbon_socket = IO::Socket::INET->new(
        PeerAddr    => $cfg{'carbon-server'},
        PeerPort    => $cfg{'carbon-port'} || 2003,
        Proto       => $cfg{'carbon-proto'},
    );
    die "unable to connect to carbon server: $!" unless defined $carbon_socket && $carbon_socket->connected;
}

#------------------------------------------------------------------------#
# Collect and Decode the Cluster Statistics
my @metrics = exists $opt{'with-cluster'} ? cluster_health() : ();
my $stats = es_node_stats('_local');
if( !$stats ) {
    output({color=>'red'}, "Error retrieving nodes_stats()");
    exit 1;
}
push @metrics, @{ parse_nodes_stats($stats) };

# Collect individual indexes names and their own statistics
if( exists $cfg{'with-indices'} ) {
    my $data = es_index_stats('_all');
    if( defined $data ) {
        push @metrics, dynamic_stat_collector($data->{indices},'cluster',$CLUSTER);
    }
    else {
        output({color=>'red'}, "Index stats requested, but response was empty.");
    }
}

#------------------------------------------------------------------------#
# Send output to appropriate channels
for ( @metrics ) {
    # Format
    my $prefix = exists $cfg{prefix} ? $cfg{prefix} : join('.', $cfg{'carbon-base'}, $HOSTNAME);
    s/^/$prefix./ unless $cfg{'no-prefix'};
    s/$/ $TIME\n/;

    # Send the Data
    if( defined $carbon_socket && $carbon_socket->connected) {
        $carbon_socket->send( $_ );
        verbose($_);
    }
    else {
        output({data=>1},$_);
    }
}

#------------------------------------------------------------------------#
# Basic Cluster Statistics
sub cluster_health {
    my $result = es_request('_cluster/health');
    my @stats =();
    if( defined $result ) {
        $CLUSTER ||= $result->{cluster_name};
        push @stats,
            "cluster.nodes.total $result->{number_of_nodes}",
            "cluster.nodes.data $result->{number_of_data_nodes}",
            "cluster.shards.primary $result->{active_primary_shards}",
            "cluster.shards.active $result->{active_shards}",
            "cluster.shards.initializing $result->{initializing_shards}",
            "cluster.shards.relocating $result->{relocating_shards}",
            "cluster.shards.unassigned $result->{unassigned_shards}",
            ;
    }
    push @stats, index_blocks();
    return @stats;
}
#------------------------------------------------------------------------#
# Index Blocks
sub index_blocks {
    my $result = es_request('_settings/index.blocks.*', { index => '_all', uri_param => { flat_settings => 'true' } });

    my %collected=();
    foreach my $idx ( keys %{ $result } ) {
        if( $result->{$idx}{settings} ) {
            foreach my $block ( keys %{ $result->{$idx}{settings} } ) {
                my $value = $result->{$idx}{settings}{$block};
                if( lc $value eq 'true') {
                    $collected{$block} ||= 0;
                    $collected{$block}++;
                }
            }
        }
    }

    return map { "cluster.$_ $collected{$_}" } sort keys %collected;
}
#------------------------------------------------------------------------#
# Parse Statistics Dynamically
sub dynamic_stat_collector {
    my $ref = shift;
    my @path = @_;
    my @stats = ();

    # Base Case
    return unless is_hashref($ref);

    foreach my $key (sort keys %{ $ref }) {
        # Skip uninteresting keys
        next if exists $_IGNORE{$key};

        # Skip peak values, we'll see those in the graphs.
        next if $key =~ /^peak/;

        # Sanitize Key Name
        my $key_name = $key;
        $key_name =~ s/(?:time_)?in_millis/ms/;
        $key_name =~ s/(?:size_)?in_bytes/bytes/;
        $key_name =~ s/\./_/g;

        if( is_hashref($ref->{$key}) ) {
            # Recurse
            push @stats, dynamic_stat_collector($ref->{$key},@path,$key_name);
        }
        elsif( $ref->{$key} =~ /^\d+(?:\.\d+)?$/ ) {
            # Numeric
            push @stats, join('.',@path,$key_name) . " $ref->{$key}";
        }
    }

    return @stats;
}

#------------------------------------------------------------------------#
# Generate Nodes Statistics
sub parse_nodes_stats {
    my $data = shift;

    # We are using _local, so we'll only have our target
    # nodes data in the results, using the loop to grab
    # the node_id, which is hashed.
    my $node_id;
    foreach my $id (keys %{ $data->{nodes} }) {
        $node_id = $id;
        $HOSTNAME=$data->{nodes}{$id}{name};
        last;
    }
    $CLUSTER ||= $data->{cluster_name};
    verbose("[$CLUSTER] Parsing node_stats for ID:$node_id => $HOSTNAME");
    my $node = $data->{nodes}{$node_id};

    my @stats = dynamic_stat_collector($node);
    return \@stats;
}

__END__

=head1 SYNOPSIS

es-graphite-dynamic.pl --host [host] [options]

Options:

    --help              print help
    --manual            print full manual
    --carbon-base       The prefix to use for carbon metrics (Default: general.es)
    --carbon-server     Send Graphite stats to Carbon Server (Automatically sets format=graphite)
    --carbon-port       Port for to use for Carbon (Default: 2003)
    --carbon-proto      Protocol for to use for Carbon (Default: tcp)
    --with-indices      Also send individual index stats
    --ignore            Comma separated list of keys to ignore in collection
    --prefix            A metric path to prefix stats, defaults to (--carbon-base).(hostname)
    --no-prefix         Don't prefix the metrics at all

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<carbon-base>

The prefix to use for metrics sent to carbon.  The default is "general.es".  Please note, the host name
of the ElasticSearch node will be appended, followed by the metric name.

=item B<carbon-server>

Send stats to the carbon server specified.  This automatically forces --format=graphite
and does not produce stats on STDOUT

=item B<carbon-port>

Use this port for the carbon server, useless without --carbon-server

=item B<with-indices>

Also grab data at the individual index level, will not append hostnames as this is useless.  It will
map the data into "$CARBON_BASE.cluster.$CLUSTERNAME.$INDEX..."

=item B<ignore>

A comma separated list of keys to ignore when parsing the tree.  This is in addition to the
default ignore list: attributes,id,timestamp,upms,_all,_shards

Examples:

    es-graphite-dynamic.pl --with-indices --ignore primaries,get,warmer

=item B<prefix>

A metric path to prefix the collected stats with.  This is useful for using es-graphite-dynamic.pl with another
collector such as Diamond which expects metrics in a certain format.  To use with diamond's userscripts or files collector
you could:

    #!/bin/sh
    # userscripts

    es-graphite-dynamic.pl --local --prefix elasticsearch

Or with the file collector, you could cron this:

    es-graphite-dynamic.pl --loocal --prefix elasticsearch --data-file /tmp/diamond/elasticsearch.out --quiet


If not specified, the assumption is data will be going directly to graphite and the metric path will be set as:

    'general.es.$HOSTNAME'

=item B<no-prefix>

Don't set the prefix to the metric names.

=back

=head1 DESCRIPTION

This script collects interesting monitoring data from the ElasticSearch cluster and maps
that data directly into Graphite.  If ElasticSearch changes the names of data, those changes
will be reflected in the metric path immediately.

=cut
