#!/usr/bin/env perl
# PODNAME: es-status.pl
# ABSTRACT: Simple ElaticSearch Status Checks
use strict;
use warnings;

use Getopt::Long qw(:config posix_default no_ignore_case no_ignore_case_always);
use Pod::Usage;
use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(:all);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    # DISPATCH FUNCTIONS
    'all',
    'health',
    'node',
    'segments',
    'settings',
    # DOCS
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Dispatch Table
my @DISPATCH = (
    { name => 'health',   handler => \&handle_health },
    { name => 'node',     handler => \&handle_node },
    { name => 'segments', handler => \&handle_segments },
    { name => 'settings', handler => \&handle_settings },

);

my @TODO=();
# Set the checks specified
foreach my $d (@DISPATCH) {
    my $parm = $d->{name};
    if( exists $opt{all} && $opt{all} ) {
        push @TODO, $parm;
    }
    elsif( exists $opt{$parm} && defined $opt{$parm} ) {
        push @TODO, $parm;
    }
}
# Default is a check
push @TODO, 'health' unless scalar @TODO;

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

#------------------------------------------------------------------------#
# Dispatch
my $ES = es_connect();

#------------------------------------------------------------------------#
# Dispatch
foreach my $action (@TODO) {
    foreach my $dispatch (@DISPATCH) {
        next unless $dispatch->{name} eq $action;
        $dispatch->{handler}->();
    }
}

#------------------------------------------------------------------------#
exit 0;
#------------------------------------------------------------------------#
# Query functions
sub handle_health {
    my $stats = es_request('_cluster/health');

    output({clear=>1,color=>"cyan"}, "Cluster Health Check", "-="x20);
    output({kv=>1,color=>"cyan"}, "name", $stats->{cluster_name});
    output({kv=>1,color=>$stats->{status}}, "health", $stats->{status});
    verbose({kv=>1}, "nodes", $stats->{number_of_nodes});

    if( $stats->{status} ne "green" ) {
        output({kv=>1,color=>"red"}, "shards_unassigned", $stats->{unassigned_shards});
        output({kv=>1,color=>"magenta"}, "shards_relocating", $stats->{relocating_shards});
        output({kv=>1,color=>"yellow"}, "shards_initializing", $stats->{initializing_shards});
    }
    else {
        verbose({kv=>1}, "shards_unassigned", $stats->{unassigned_shards});
        verbose({kv=>1}, "shards_relocating", $stats->{relocating_shards});
        verbose({kv=>1}, "shards_initializing", $stats->{initializing_shards});
    }
}
sub handle_segments {
    output({clear=>1,color=>"cyan"}, "Index Segmentation Check", "-="x20);

    my @indexes = es_indices();
    foreach my $index ( sort @indexes ) {
        output({color=>"cyan"},"$index:");
        my $stats = es_index_segments($index);
        my $shards = 0;
        my $segments = 0;
        my $index_size = 0;
        foreach my $id (sort keys %{ $stats->{indices}{$index}{shards} } ) {
            $shards++;
            verbose({kv=>1,color=>"magenta",indent=>1}, "shard", $id);
            my $shard = $stats->{indices}{$index}{shards}{$id}[0];
            my $color = $shard->{num_search_segments} > 1 ? 'yellow' : 'green';
            $segments += $shard->{num_search_segments};
            verbose({kv=>1,color=>$color,indent=>2}, "segments", $shard->{num_search_segments} );
            my $size = 0;
            foreach my $seg ( keys %{ $shard->{segments} }) {
                $size += $shard->{segments}{$seg}{size_in_bytes};
            }
            verbose({kv=>1,indent=>2}, "size_bytes", $size);
            my @units = qw(kb mb gb tb);
            my $unit = 'b';
            my $size_short = $size;
            while( $size_short > 1024 && @units ) {
                $size_short /= 1024;
                $unit = shift @units;
            }
            verbose({kv=>1,indent=>2}, "size", sprintf("%.2f %s", $size_short, $unit) );
            $index_size += $size;
        }
        my $ratio = $shards > 0 ? sprintf("%.2f", $segments / $shards ) : 0;
        my $color = $ratio == 1 ? "green" : "yellow";
        output({kv=>1,color=>$color,indent=>1}, "segments_to_shards", $ratio);
        # Index size
        my @units = qw(kb mb gb tb);
        my $unit = 'b';
        my $size_short = $index_size;
        while( $size_short > 1024 && @units ) {
            $size_short /= 1024;
            $unit = shift @units;
        }
        verbose({kv=>1,indent=>1}, "index_size", sprintf("%.2f %s", $size_short, $unit) );
        verbose({kv=>1,indent=>1,level=>2}, "index_size_bytes", $index_size );
    }
}

sub handle_node {
    my $stats = es_node_stats();
    output({clear=>1,color=>"cyan"}, "Node Status Check", "-="x20);

    my $node_id = (keys %{ $stats->{nodes} })[0];
    my $node = $stats->{nodes}{$node_id};
    output({kv=>1,color=>"cyan"}, "name", $node->{name});
    output({kv=>1}, "index_size", $node->{indices}{store}{size} );
    verbose({kv=>1}, "index_size_bytes", $node->{indices}{store}{size_in_bytes} );
    verbose({kv=>1}, "docs", $node->{indices}{docs}{count} );
    output({kv=>1}, "open_fd", $node->{process}{open_file_descriptors} );

    # JVM Stats
    output({kv=>1}, "jvm", '');
    # Threads
    output({indent=>1,kv=>1}, "threads_current", $node->{jvm}{threads}{count});
    output({indent=>1,kv=>1}, "threads_peak", $node->{jvm}{threads}{peak_count});
    # Memory
    output({indent=>1,kv=>1}, "mem", '');
    output({indent=>2,kv=>1,color=>"yellow"}, "heap_used", $node->{jvm}{mem}{heap_used});
    verbose({indent=>2,kv=>1,color=>"yellow"}, "heap_used_bytes", $node->{jvm}{mem}{heap_used_in_bytes});
    output({indent=>2,kv=>1}, "heap_committed", $node->{jvm}{mem}{heap_committed});
    verbose({indent=>2,kv=>1}, "heap_committed_bytes", $node->{jvm}{mem}{heap_committed_in_bytes});
    # GC
    output({indent=>1,kv=>1}, "gc", '');
    # GC Details
    foreach my $collector ( keys %{ $node->{jvm}{gc}{collectors} } ) {
        output({indent=>2,kv=>1}, $collector, '');
        output({indent=>3,kv=>1}, "collections", $node->{jvm}{gc}{collectors}{$collector}{collection_count} );
        output({indent=>3,kv=>1}, "time", $node->{jvm}{gc}{collectors}{$collector}{collection_time} );
        output({indent=>3,kv=>1}, "time_ms", $node->{jvm}{gc}{collectors}{$collector}{collection_time_in_millis} );
    }
    output({kv=>1}, "requests", $node->{transport}{rx_count});
    output({indent=>1,kv=>1}, "rx", $node->{transport}{rx_size});
    verbose({indent=>1,kv=>1}, "rx_bytes", $node->{transport}{rx_size_in_bytes});
    output({kv=>1}, "responses", $node->{transport}{tx_count});
    output({indent=>1,kv=>1}, "tx", $node->{transport}{tx_size});
    verbose({indent=>1,kv=>1}, "tx_bytes", $node->{transport}{tx_size_in_bytes});
}

sub handle_settings {
    my $stats = es_settings();
    output({clear=>1,color=>"cyan"}, "Index Settings Check", "-="x20);

    my $colorize = sub {
        my ($v) = shift;
        return "green" if $v eq 'false';
        return "yellow" if $v eq 'not set';
        return "red";
    };

    foreach my $index (sort keys %{ $stats } ) {
        my %settings = %{ $stats->{$index}{settings} };
        output({color=>'cyan'}, "$index:");
        my $value = exists $settings{'index.auto_expand_replicas'} && defined $settings{'index.auto_expand_replicas'} ? $settings{'index.auto_expand_replicas'} : 'not set';
        my $color = $colorize->($value);
        output({indent=>1,kv=>1,color=>$color}, "auto_expand_replicas", $value);
        verbose({indent=>1,kv=>1}, "replicas", $settings{'index.number_of_replicas'});
        verbose({indent=>1,kv=>1}, "shards", $settings{'index.number_of_shards'});
    }
}

__END__

=head1 SYNOPSIS

es-status.pl --health --verbose --color

Options:
    --help              print help
    --manual            print full manual

Query Modes:
    --health            Display overall cluster health (--verbose shows more detail)
    --node              Display node details (--verbose shows more detail)
    --segments          Display segmentation details (--verbose shows more detail)
    --settings          Display index settings (--verbose shows more detail)

    --all               Run all handlers!

=from_other App::ElasticSearch::Utilities / ARGS / ALL

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<all>

Runs all the ES Checks at once, this includes: health, node, and segments
Used in combination with --verbose for more detail

    ./es-status.pl --all --verbose

=item B<health>

Displays the cluster health, use --verbose to get more information

    ./es-status.pl --health --verbose

=item B<node>

Displays the details on index segmentation, use --verbose to get more information

    ./es-status.pl --node --verbose

=item B<segments>

Displays the details on index segmentation, use --verbose to get more information

    ./es-status.pl --segments --verbose

=item B<settings>

Displays the details on index settings, use --verbose to get more information

    ./es-status.pl --settings --verbose

=head1 DESCRIPTION

This script is designed to help you get information about the state of the
ElasticSearh cluster in a hurry.

=cut
