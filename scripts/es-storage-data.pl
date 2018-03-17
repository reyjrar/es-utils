#!perl
# PODNAME: es-storage-data.pl
# ABSTRACT: Index pattern-aware elasticsearch storage statistics
use strict;
use warnings;
use feature qw(state);

use App::ElasticSearch::Utilities qw(es_request es_pattern es_nodes es_indices);
use CLI::Helpers qw(:output);
use Getopt::Long::Descriptive;
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
my ($opt,$usage) = describe_options('%c %o',
    ['sort:s',  "sort by name or size, default: name",
            { default => 'name', callbacks => { 'must be name or size' => sub { $_[0] eq 'name' || $_[0] eq 'size' } } }
    ],
    ['view:s',  "Show by index or node, default: node",
          { default => 'node', callbacks => { 'must be index or node' => sub { $_[0] eq 'node' || $_[0] eq 'index' } } }
    ],
    ['asc',     "Sort ascending  (default by name)"],
    ['desc',    "Sort descending (default by size)"],
    ['limit:i', "Limit to showing only this many, ie top N", { default => 0 }],
    ['raw',     "Display numeric data without rollups"],
    [],
    ['help|h',  "Display this help", { shortcircuit => 1 }],
    ['manual|m',"Display the full manual", { shortcircuit => 1 }],
);

#------------------------------------------------------------------------#
# Documentations!
if( $opt->help ) {
    print $usage->text;
    exit;
}
pod2usage(-exitstatus => 0, -verbose => 2) if $opt->manual;

# Get the pattern we're using
my $PATTERN = es_pattern();

# Indices and Nodes
my @INDICES = es_indices();
my %NODES   = es_nodes();

# Loop through the indices and take appropriate actions;
my %indices = ();
my %nodes = ();
foreach my $index (@INDICES) {
    verbose({color=>'green'}, "$index - Gathering statistics");

    my $result = es_request('_status', { index => $index });
    if( !defined $result ) {
        output({color=>'magenta',indent=>1}, "+ Unable to fetch index status!");
        next;
    }
    verbose({indent=>1}, "+ Succesful");
    my $status = $result->{indices}{$index}{primaries};
    debug("index_status( $index ");
    debug_var($status);

    # Grab Index Data
    $indices{$index} = {
        size        => $status->{store}{size_in_bytes},
        docs        => $status->{docs}{count},
    };

    my $shards = es_request("_cat/shards",
        { index => $index, uri_param => { qw(bytes b format json) }}
    );

    my %shards = ();
    foreach my $s (@{ $shards }) {
        my $node = $s->{node};
        $shards{$s->{shard}} ||= {};
        $nodes{$node}   ||= {};

        if( exists $shards{$s->{shard}}->{$node} ) {
            $shards{$s->{shard}}->{$node}{size} += $s->{store};
            $shards{$s->{shard}}->{$node}{docs} += $s->{docs};
        }
        else {
            $shards{$s->{shard}}->{$node} = {
                size => $s->{store},
                docs => $s->{docs},
            };
        }
        no warnings;
        $nodes{$node}->{$_} ||= 0 for qw(size shards docs);
        $nodes{$node}->{size} += $s->{store};
        $nodes{$node}->{shards}++;
        $nodes{$node}->{docs} += $s->{docs};
    }
    $indices{$index}->{shards} = \%shards;
}

output({color=>'white'}, sprintf "Storage data for %s from indices matching '%s'", $opt->view, $PATTERN->{string});
if( $opt->view eq 'index' ) {
    my $displayed = 0;
    foreach my $index (sort indices_by keys %indices) {
        output({color=>"magenta",indent=>1}, $index);
        output({color=>"cyan",kv=>1,indent=>2}, 'size', pretty_size( $indices{$index}->{size}));
        output({color=>"cyan",kv=>1,indent=>2}, 'docs', $indices{$index}->{docs});
        $displayed++;
        last if $opt->limit > 0 && $displayed >= $opt->limit;
    }
}
elsif( $opt->view eq 'node' ) {
    my $displayed = 0;
    foreach my $node (sort nodes_by keys %nodes) {
        output({color=>"magenta",indent=>1}, $node);
        output({color=>"cyan",kv=>1,indent=>2}, 'size', pretty_size( $nodes{$node}->{size}));
        output({color=>"cyan",kv=>1,indent=>2}, 'shards',  $nodes{$node}->{shards});
        output({color=>"cyan",kv=>1,indent=>2}, 'docs',  $nodes{$node}->{docs});
        $displayed++;
        last if $opt->limit > 0 && $displayed >= $opt->limit;
    }
}

exit (0);

sub pretty_size {
    my ($size)=@_;
    state $warned = 0;

    my $value = $size;
    if( !$opt->raw ) {
        my @indicators = qw(kb mb gb tb);
        my $indicator = '';

        while( $size > 1024 && @indicators ) {
            $indicator = shift @indicators;
            $size /= 1024;
        }
        $value = sprintf('%0.2f %s', $size, $indicator);
    }

    return $value;
}

sub indices_by {
    if( $opt->sort eq 'size' ) {
        return $opt->asc ?
            $indices{$a}->{size} <=> $indices{$b}->{size} :
            $indices{$b}->{size} <=> $indices{$a}->{size} ;
    }
    return $opt->desc ? $b cmp $a : $a cmp $b;
}

sub nodes_by {
    if( $opt->sort eq 'size' ) {
        return $opt->asc ?
            $nodes{$a}->{size} <=> $nodes{$b}->{size} :
            $nodes{$b}->{size} <=> $nodes{$a}->{size} ;
    }
    return $opt->desc ? $b cmp $a : $a cmp $b;
}


__END__

=head1 SYNOPSIS

es-storage-data.pl --local --pattern logstash-* shards

Options:

    --help              print help
    --manual            print full manual
    --view              Show by node or index, default node
    --format            Output format for numeric data, pretty(default) or raw
    --sort              Sort by, name(default) or size
    --limit             Show only the top N, default no limit
    --asc               Sort ascending
    --desc              Sort descending (default)

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<view>

Default view is by node, but can also be index to see statistics by index

=item B<sort>

How to sort the data, by it's name (the default) or size

=item B<limit>

Show only the first N items, or everything is N == 0

=item B<asc>

Sort ascending, the default for name

=item B<desc>

Sort descending, the default for size

=back

=head1 DESCRIPTION

This script allows you view the storage statistics of the ElasticSearch cluster.

Usage:

    # Show usage data for nodes with logstash indices
    $ es-storage-data.pl --local --pattern logstash-*

    # Show the top 10 largest indices
    $ es-storage-data.pl --local --view index --limit 10 --sort size

    # Show the "newest" logstash index
    $ es-storage-data.pl --local --view index --limit 1

=cut
