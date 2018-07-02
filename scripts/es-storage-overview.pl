#!perl
# PODNAME: es-storage-overview.pl
# ABSTRACT: Index Storage Overview by Index Name without Dates
use strict;
use warnings;
use feature qw(state);

use App::ElasticSearch::Utilities qw(es_request es_index_strip_date es_index_bases);
use CHI;
use CLI::Helpers qw(:output);
use Getopt::Long::Descriptive;
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
my ($opt,$usage) = describe_options('%c %o',
    ['sort:s',  "sort by name or size, default: name",
            { default => 'name', callbacks => { 'must be name or size' => sub { $_[0] =~ /^name|size$/ } } }
    ],
    ['asc',     "Sort ascending  (default by name)"],
    ['desc',    "Sort descending (default by size)"],
    ['limit:i', "Limit to showing only this many, ie top N", { default => 0 }],
    ['raw',     "Display numeric data without rollups"],
    [],
    ['clear-cache', "Clear the _cat/indices cache"],
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

my $cache = CHI->new(
    driver    => 'File',
    root_dir  => "$ENV{HOME}/.es-utils/cache",
    namespace => 'storage',
    expires_in => '10min',
);

$cache->clear if $opt->clear_cache;

my %indices = ();
my %bases = ();
my %overview = (
    shards  => 0,
    indices => 0,
    docs    => 0,
    size    => 0,
    memory  => 0,
);

my %fields = qw(
    index index
    pri   shards
    rep   shards
    docs.count docs
    store.size size
    memory.total memory
);
my $result = $cache->get('_cat/indices');
if( !defined $result ) {
    output({color=>'cyan'}, "Fetching Index Meta-Data");
    $result  ||= es_request('_cat/indices',
            {
                uri_param => {
                    h      => join(',', sort keys %fields),
                    bytes  => 'b',
                    format => 'json'
                }
            }
    );
    $cache->set('_cat/indices', $result);
}

foreach my $row (@{ $result }) {
    # Index Name
    my $index = delete $row->{index};

    $overview{indices}++;
    verbose({color=>'green'}, "$index - Gathering statistics");

    my @bases = es_index_strip_date($index);
    foreach my $base ( @bases ) {
        # Count Indexes
        $bases{$base}->{indices} ||= 0;
        $bases{$base}->{indices}++;
        # Handle keys
        foreach my $k (keys %{ $row }) {
            next unless exists $fields{$k};
            my $dk = $fields{$k};
            # Grab Overview Data
            $overview{$dk} += $row->{$k};
            # Counts against bases
            $bases{$base} ||=  {};
            $bases{$base}->{$dk} ||= 0;
            $bases{$base}->{$dk} += $row->{$k};
        }
    }
}

output({color=>'white'}, "Storage Overview");
my $displayed = 0;
foreach my $index (sort indices_by keys %bases) {
    output({color=>"magenta",indent=>1}, $index);
    output({color=>"cyan",kv=>1,indent=>2}, 'size',    pretty_size( $bases{$index}->{size}));
    output({color=>"cyan",kv=>1,indent=>2}, 'indices', $bases{$index}->{indices});
    output({color=>"cyan",kv=>1,indent=>2}, 'avgsize', pretty_size( $bases{$index}->{size} / $bases{$index}->{indices} ));
    output({color=>"cyan",kv=>1,indent=>2}, 'shards',  $bases{$index}->{shards});
    output({color=>"cyan",kv=>1,indent=>2}, 'docs',    $bases{$index}->{docs});
    output({color=>"cyan",kv=>1,indent=>2}, 'memory',  pretty_size( $bases{$index}->{memory}));
    $displayed++;
    last if $opt->limit > 0 && $displayed >= $opt->limit;
}
output({color=>'white',clear=>1},"Total for scanned data");
    output({color=>"cyan",kv=>1,indent=>1}, 'size',    pretty_size( $overview{size}));
    output({color=>"cyan",kv=>1,indent=>1}, 'indices', $overview{indices});
    output({color=>"cyan",kv=>1,indent=>1}, 'shards',  $overview{shards});
    output({color=>"cyan",kv=>1,indent=>1}, 'docs',    $overview{docs});
    output({color=>"cyan",kv=>1,indent=>1}, 'memory',  pretty_size( $overview{memory}));


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
            $bases{$a}->{size} <=> $bases{$b}->{size} :
            $bases{$b}->{size} <=> $bases{$a}->{size} ;
    }
    return $opt->desc ? $b cmp $a : $a cmp $b;
}

__END__

=head1 SYNOPSIS

es-storage-overview.pl --local

Options:

    --help              print help
    --manual            print full manual
    --clear-cache       Clear the cached statistics, they only live for a few
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
    $ es-storage-overview.pl --local


=cut
