#!/usr/bin/env perl
# PODNAME: es-graphite-static.pl
# ABSTRACT: Gather performance metrics from an ES node and send them to Graphite
use strict;
use warnings;

BEGIN {
    # We don't want to use the proxies set in our environment
    delete $ENV{$_} for qw(http_proxy HTTP_PROXY https_proxy HTTPS_PROXY);
}

use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(es_connect es_node_stats es_index_stats);
use IO::Socket::INET;
use Getopt::Long qw(:config posix_default no_ignore_case no_ignore_case_always);
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'format:s',
    'carbon-base:s',
    'carbon-proto:s',
    'carbon-server:s',
    'carbon-port:i',
    'with-indices',
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

#------------------------------------------------------------------------#
# Argument Sanitazation
my %_formats = (
    carbon      => 'graphite',
    graphite    => 'graphite',
    cacti       => 'cacti',
);
 # Force graphite if carbon-server specified
if( exists $opt{'carbon-server'} and length $opt{'carbon-server'} ) {
    $opt{format} = 'graphite';
}
# Validate Format
if( exists $opt{format} and length $opt{format} ) {
    if( exists $_formats{$opt{format}} ) {
        $opt{format} = $_formats{$opt{format}};
    }
    else {
        delete $opt{format};
    }
}
# Merge options into config
my %cfg = (
    format => 'graphite',
    'carbon-proto' => 'tcp',
    'carbon-base'  => 'general.es',
    %opt,
    host => App::ElasticSearch::Utilities::def('HOST')
);

#------------------------------------------------------------------------#
# Format Routines
my $time = time;
my $HOSTNAME = undef;
my %_formatter = (
    cacti       => sub {
            local $_ = shift;
            my $name = shift;
            s/\./_/g;
            s/\s/:/;
            s/$/\n/;
            $_;
    },
    graphite    => sub {
            local $_ = shift;
            s/^/$cfg{'carbon-base'}.$HOSTNAME./;
            s/$/ $time\n/;
            $_;
    },
);

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
my @metrics = ();
my $stats = es_node_stats('_local');
if( !$stats ) {
    output({color=>'red'}, "Error retrieving nodes_stats()");
    exit 1;
}
debug_var({color=>'yellow'}, $stats);
push @metrics, @{ parse_nodes_stats($stats) };

# Collect individual indexes names and their own statistics
if( exists $cfg{'with-indices'} ) {
    my $index_stats = undef;
    eval {
        $index_stats = es_index_stats('_all');
        debug_var({color=>'yellow'}, $index_stats);
    };
    push @metrics, @{ parse_index_stats( $index_stats ) };
}

#------------------------------------------------------------------------#
# Send output to appropriate channels
foreach my $stat ( @metrics ) {
    my $output = format_output( $stat );
    if( defined $carbon_socket && $carbon_socket->connected) {
        $carbon_socket->send( $output );
        verbose($output);
    }
    else {
        output($output);
    }
}


#------------------------------------------------------------------------#
# Generate Node Statistics Hash
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
    verbose("Parsing node_stats for ID:$node_id => $HOSTNAME");
    my $node = $data->{nodes}{$node_id};

    my @stats = ();
    # Index Details
    push @stats,
        # Basic Stats
        "indices.size $node->{indices}{store}{size_in_bytes}",
        "indices.docs $node->{indices}{docs}{count}",
        # Indexing
        "indices.indexing.total $node->{indices}{indexing}{index_total}",
        "indices.indexing.total_ms $node->{indices}{indexing}{index_time_in_millis}",
        "indices.indexing.delete $node->{indices}{indexing}{delete_total}",
        "indices.indexing.delete_ms $node->{indices}{indexing}{delete_time_in_millis}",
         # Get Data
        "indices.get.total $node->{indices}{get}{total}",
        "indices.get.total_ms $node->{indices}{get}{time_in_millis}",
        "indices.get.exists $node->{indices}{get}{exists_total}",
        "indices.get.exists_ms $node->{indices}{get}{exists_time_in_millis}",
        "indices.get.missing $node->{indices}{get}{missing_total}",
        "indices.get.missing_ms $node->{indices}{get}{missing_time_in_millis}",
        # Search Data
        "indices.search.query $node->{indices}{search}{query_total}",
        "indices.search.query_ms $node->{indices}{search}{query_time_in_millis}",
        "indices.search.fetch $node->{indices}{search}{fetch_total}",
        "indices.search.fetch_ms $node->{indices}{search}{fetch_time_in_millis}",
        # Search Data
        "indices.cache.field_evictions $node->{indices}{cache}{field_evictions}",
        "indices.cache.field_size $node->{indices}{cache}{field_size_in_bytes}",
        "indices.cache.filter_evictions $node->{indices}{cache}{filter_evictions}",
        "indices.cache.filter_size $node->{indices}{cache}{filter_size_in_bytes}",
        # Merges
        "indices.merges.total_docs $node->{indices}{merges}{total_docs}",
        "indices.merges.total_size $node->{indices}{merges}{total_size_in_bytes}",
        "indices.merges.total_ms $node->{indices}{merges}{total_time_in_millis}",
        # Refresh
        "indices.refresh.total $node->{indices}{refresh}{total}",
        "indices.refresh.total_ms $node->{indices}{refresh}{total_time_in_millis}",
        # Flush
        "indices.flush.total $node->{indices}{flush}{total}",
        "indices.flush.total_ms $node->{indices}{flush}{total_time_in_millis}",
        # Field Data
        "indices.fielddata.evictions $node->{indices}{fielddata}{evictions}",
        "indices.fielddata.size $node->{indices}{fielddata}{memory_size_in_bytes}",
        # Filter Cache
        "indices.filter.evictions $node->{indices}{filter_cache}{evictions}",
        "indices.filter.size $node->{indices}{filter_cache}{memory_size_in_bytes}",
        # ID Cache
        "indices.id.size $node->{indices}{id_cache}{memory_size_in_bytes}",
        ;

    # Transport Details
    push @stats,
        "transport.rx_bytes $node->{transport}{rx_size_in_bytes}",
        "transport.rx_count $node->{transport}{rx_count}",
        "transport.tx_bytes $node->{transport}{tx_size_in_bytes}",
        "transport.tx_count $node->{transport}{tx_count}",
        "transport.server_open $node->{transport}{server_open}",
        ;
    # HTTP Details
    push @stats,
        "http.open $node->{http}{current_open}",
        "http.total $node->{http}{total_opened}",
        ;
    # JVM Garbage Collectors;
    push @stats,
        "jvm.gc.count $node->{jvm}{gc}{collection_count}",
        "jvm.gc.time_ms $node->{jvm}{gc}{collection_time_in_millis}",
        ;
    foreach my $collector (keys %{ $node->{jvm}{gc}{collectors} } ) {
        my $col = $node->{jvm}{gc}{collectors}{$collector};
        my $prefix = "jvm.gc.collector.$collector";
        push @stats,
            "$prefix.count $col->{collection_count}",
            "$prefix.time_ms $col->{collection_time_in_millis}",
            ;
    }
    # JVM Memory Usage
    my %_mem = ( used_bytes => 'used_in_bytes', committed_bytes => 'committed_in_bytes' );
    foreach my $heap (qw(heap non_heap)) {
        while( my ($gm,$em) = each %_mem ) {
            my $val = $node->{jvm}{mem}{"${heap}_${em}"};
            push @stats,
                "jvm.mem.$heap.$gm $val";
        }
    }
    # GC Pools
    my %_pool = ( used_bytes => 'used_in_bytes', max_bytes => 'max_in_bytes' );
    foreach my $gcpool ( keys %{ $node->{jvm}{mem}{pools} }) {
        my $name = $gcpool;
        $name =~ s/\s+//g;
        foreach my $metric (keys %_pool) {
            push @stats, "jvm.mem.pools.$name.$metric $node->{jvm}{mem}{pools}{$gcpool}{$_pool{$metric}}";
        }
    }
    # JVM Threads
    push @stats,
        "jvm.threads $node->{jvm}{threads}{count}",
        ;
    my @pools_of_interest = qw/ search bulk index generic get management /;
    foreach my $pool ( @pools_of_interest ) {
        foreach my $metric ( keys %{$node->{thread_pool}{$pool}} ) {
            push @stats,
                "jvm.thread_pool.$pool.$metric $node->{thread_pool}{$pool}{$metric}"
                ;
        }
    }
    # OS Information
    push @stats,
        "process.openfds $node->{process}{open_file_descriptors}",
        ;
    return \@stats;
}

#------------------------------------------------------------------------#
# Generate Individual Index Statistics Stats
sub parse_index_stats{
    my $data = shift;
    my $index_name;
    my @indices_stats;
    foreach my $index (keys %{ $data->{indices} }) {
        foreach my $group ("primaries", "total") {
            my $index_data = $data->{indices}{$index}{$group};

            push @indices_stats,
                # Basic Stats
                "individual_indices.$index.$group.docs.count $index_data->{docs}{count}",
                "individual_indices.$index.$group.docs.deleted $index_data->{docs}{deleted}",
                "individual_indices.$index.$group.store.size_in_bytes $index_data->{store}{size_in_bytes}",
                "individual_indices.$index.$group.store.throttle_time_in_millis $index_data->{store}{throttle_time_in_millis}",

                # Indexing
                "individual_indices.$index.$group.indexing.index_total $index_data->{indexing}{index_total}",
                "individual_indices.$index.$group.indexing.index_time_in_millis $index_data->{indexing}{index_time_in_millis}",
                "individual_indices.$index.$group.indexing.index_current $index_data->{indexing}{index_current}",
                "individual_indices.$index.$group.indexing.delete_total $index_data->{indexing}{delete_total}",
                "individual_indices.$index.$group.indexing.delete_time_in_millis $index_data->{indexing}{delete_time_in_millis}",
                "individual_indices.$index.$group.indexing.delete_current $index_data->{indexing}{delete_current}",

                # Get
                "individual_indices.$index.$group.get.total $index_data->{get}{total}",
                "individual_indices.$index.$group.get.time_in_millis $index_data->{get}{time_in_millis}",
                "individual_indices.$index.$group.get.exists_total $index_data->{get}{exists_total}",
                "individual_indices.$index.$group.get.exists_time_in_millis $index_data->{get}{exists_time_in_millis}",
                "individual_indices.$index.$group.get.missing_total $index_data->{get}{missing_total}",
                "individual_indices.$index.$group.get.missing_time_in_millis $index_data->{get}{missing_time_in_millis}",
                "individual_indices.$index.$group.get.current $index_data->{get}{current}",

                # Search
                "individual_indices.$index.$group.search.open_contexts $index_data->{search}{open_contexts}",
                "individual_indices.$index.$group.search.query_total $index_data->{search}{query_total}",
                "individual_indices.$index.$group.search.query_time_in_millis $index_data->{search}{query_time_in_millis}",
                "individual_indices.$index.$group.search.query_current $index_data->{search}{query_current}",
                "individual_indices.$index.$group.search.fetch_total $index_data->{search}{fetch_total}",
                "individual_indices.$index.$group.search.fetch_time_in_millis $index_data->{search}{fetch_time_in_millis}",
                "individual_indices.$index.$group.search.fetch_current $index_data->{search}{fetch_current}",
            ;
        }
    }
    return \@indices_stats;
}

#------------------------------------------------------------------------#
# Formatters
sub format_output {
    my $line = shift;
    if( exists $_formatter{$cfg{format}} ) {
        return $_formatter{$cfg{format}}->( $line );
    }
    else {
        warn "call to undefined formatter($cfg{format})";
        return "$line\n";
    }
}


__END__

=head1 SYNOPSIS

es-graphite-static.pl --format=graphite --host [host] [options]

Options:

    --help              print help
    --manual            print full manual
    --format            stats Format (graphite or cacti) (Default: graphite)
    --carbon-base       The prefix to use for carbon metrics (Default: general.es)
    --carbon-server     Send Graphite stats to Carbon Server (Automatically sets format=graphite)
    --carbon-port       Port for to use for Carbon (Default: 2003)
    --carbon-proto      Protocol for to use for Carbon (Default: tcp)
    --with-indices      Also send individual index stats

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<format>

stats format:

    graphite        Use format for graphite/carbon (default)
    cacti           For use with Cacti

=item B<carbon-base>

The prefix to use for metrics sent to carbon.  The default is "general.es".  Please note, the host name
of the ElasticSearch node will be appended, followed by the metric name.

=item B<carbon-server>

Send stats to the carbon server specified.  This automatically forces --format=graphite
and does not produce stats on STDOUT

=item B<carbon-port>

Use this port for the carbon server, useless without --carbon-server

=item B<with-indices>

Also grab data at the individual index level

=back

=head1 DESCRIPTION

This script extract monitoring data from ElasticSearch and those statistics to a Graphite
end point.  It also support cacti, though support for cacti will likely be deprecated.

This script is called "static" as the author will attempt to handle statistics that are renamed
by ElasticSearch.com so what all versions of ElasticSearch will produce the same output.

=cut
