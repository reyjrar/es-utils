#!/usr/bin/env perl
# PODNAME: es-storage-data.pl
# ABSTRACT: Index pattern-aware elasticsearch storage statistics
use strict;
use warnings;
use feature qw(state);

use Getopt::Long qw(:config posix_default no_ignore_case no_ignore_case_always);
use Pod::Usage;
use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(es_request es_pattern es_nodes es_indices);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'sort:s',
    'format:s',
    'view:s',
    'close',
    'limit:i',
    'asc',
    'desc',
    # Basic options
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

# Configuration
my %CFG = (
    sort      => 'name',
    format    => 'pretty',
    view      => 'node',
    limit     => 0,
);
my %VALID = (
    format => {map { $_ => 1 } qw(pretty raw)},
    view   => {map { $_ => 1 } qw(node index)},
);
# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    if ( exists $opt{$setting} and defined $opt{$setting} ) {
        if( exists $VALID{$setting} ) {
            if(exists $VALID{$setting}->{$opt{$setting}}) {
                $CFG{$setting} = $opt{$setting};
            }
            else {
                output({color=>'red',stderr=>1},
                    "Invalid option for $setting: '$opt{$setting}', valid are: " . join(',', sort keys %{ $VALID{$setting} })
                );
            }
        }
        else {
            $CFG{$setting} = $opt{$setting};
        }
    }
}
# Get the pattern we're using
my $PATTERN = es_pattern();

# Indices and Nodes
my @INDICES = es_indices();
my %NODES = es_nodes();

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
    my $status = $result->{indices}{$index};
    debug("index_status( $index ");
    debug_var($status);

    $indices{$index} = {
        size        => $status->{index}{size_in_bytes},
        size_pretty => $status->{index}{size},
        docs        => $status->{docs}{num_docs},
    };
    my %shards = ();
    foreach my $shard (keys %{ $status->{shards} }) {
        foreach my $instance (@{ $status->{shards}{$shard} }) {
            my $node = $NODES{$instance->{routing}{node}};

            $shards{$shard} ||= {};
            $nodes{$node}   ||= {};

            if( exists $shards{$shard}->{$node} ) {
                $shards{$shard}->{$node}{size} += $instance->{index}{size_in_bytes};
                $shards{$shard}->{$node}{docs} += $instance->{docs}{num_docs};
            }
            else {
                $shards{$shard}->{$node} = {
                    size => $instance->{index}{size_in_bytes},
                    docs => $instance->{docs}{num_docs},
                };
            }
            no warnings;
            $nodes{$node}->{$_} ||= 0 for qw(size shards docs);
            $nodes{$node}->{size} += $instance->{index}{size_in_bytes};
            $nodes{$node}->{shards}++;
            $nodes{$node}->{docs} += $instance->{docs}{num_docs};
        }
    }
    $indices{$index}->{shards} = \%shards;
}

output({color=>'white'}, "Storage data for $CFG{view} from indices matching '$PATTERN->{string}'");
if( $CFG{view} eq 'index' ) {
    my $displayed = 0;
    foreach my $index (sort indices_by keys %indices) {
        output({color=>"magenta",indent=>1}, $index);
        output({color=>"cyan",kv=>1,indent=>2}, 'size', pretty_size( $indices{$index}->{size}));
        output({color=>"cyan",kv=>1,indent=>2}, 'docs', $indices{$index}->{docs});
        $displayed++;
        last if $CFG{limit} > 0 && $displayed >= $CFG{limit};
    }
}
elsif( $CFG{view} eq 'node' ) {
    my $displayed = 0;
    foreach my $node (sort nodes_by keys %nodes) {
        output({color=>"magenta",indent=>1}, $node);
        output({color=>"cyan",kv=>1,indent=>2}, 'size', pretty_size( $nodes{$node}->{size}));
        output({color=>"cyan",kv=>1,indent=>2}, 'shards',  $nodes{$node}->{shards});
        output({color=>"cyan",kv=>1,indent=>2}, 'docs',  $nodes{$node}->{docs});
        $displayed++;
        last if $CFG{limit} > 0 && $displayed >= $CFG{limit};
    }
}

exit (0);

sub pretty_size {
    my ($size)=@_;
    state $warned = 0;

    my $value = $size;
    if( $CFG{format} eq 'pretty' ) {
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
    if( exists $opt{sort} && $opt{sort} eq 'size' ) {
        return exists $opt{asc} ?
            $indices{$a}->{size} <=> $indices{$b}->{size} :
            $indices{$b}->{size} <=> $indices{$a}->{size} ;
    }
    return exists $opt{asc} ? $a cmp $b : $b cmp $a;
}

sub nodes_by {
    if( exists $opt{sort} && $opt{sort} eq 'size' ) {
        return $nodes{$b}->{size} <=> $nodes{$a}->{size};
    }
    return exists $opt{asc} ? $a cmp $b : $b cmp $a;
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

Sort ascending

=item B<desc>

Sort descending, the default

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
