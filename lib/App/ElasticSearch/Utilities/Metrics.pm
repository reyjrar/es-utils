package App::ElasticSearch::Utilities::Metrics;
# ABSTRACT: Fetches performance metrics about the node

=head1 SYNOPSIS

This provides a simple API to export some core metrics from the local
ElasticSearch instance.

    use App::ElasticSearch::Utilities qw(es_connect);
    use App::ElasticSearch::Utilities::Metrics;

    my $metrics_fetcher = App::ElasticSearch::Utilities::Metrics->new(
        connection           => es_connect(),
        with_cluster_metrics => 1,
        with_index_metrics   => 1,
    );

    my $metrics = $metrics_fetcher->get_metrics();

=cut

use v5.16;
use warnings;

# VERSION

use App::ElasticSearch::Utilities qw(es_connect);
use CLI::Helpers qw(:output);
use JSON::MaybeXS;
use Ref::Util qw(is_ref is_arrayref is_hashref);
use Types::Standard qw( ArrayRef Bool HashRef InstanceOf Int Str );

use Moo;
use namespace::autoclean;

=attr connection

An `App::ElasticSearch::Utilities::Connection` instance, or automatically
created via C<es_connect()>.

=cut

has 'connection' => (
    is      => 'ro',
    isa     => InstanceOf['App::ElasticSearch::Utilities::Connection'],
    default => sub { es_connect() },
    handles => [qw(host port request)],
);

my @_IGNORES = qw(
    _all _shards
    attributes id timestamp uptime_in_millis
);

=attr ignore

An array of metric names to ignore, in addition to the static list when parsing
the `_node/_local/stats` stats.  Defaults to:

    [qw(adaptive_selection)]

=cut

has 'ignore' => (
    is      => 'lazy',
    isa     => ArrayRef[Str],
    default => sub { [qw(adaptive_selection)] },
);

=attr node_details

The Node details provided by the C<_nodes/_local> API.

=cut

has 'node_details' => (
    is => 'lazy',
    isa => HashRef,
    init_arg => undef,
);

sub _build_node_details {
    my ($self) = @_;

    if( my $res = $self->request('_nodes/_local')->content ) {
        if( my $nodes = $res->{nodes} ) {
            my ($id) = keys %{ $nodes };
            return {
                %{ $nodes->{$id} },
                id => $id,
            }
        }
    }

    # Fail our type check
    return;
}

=attr node_id

The Node ID for the connection, will be automatically discovered

=cut

has 'node_id' => (
    is  => 'lazy',
    isa => Str,
);

sub _build_node_id {
    my ($self) = @_;

    if( my $details = $self->node_details ) {
        return $details->{id};
    }

    warn sprintf "unable to determine node_id for %s:%d",
        $self->host, $self->port;

    # Fail our type check
    return;
}

=attr with_cluster_metrics

Boolean, set to true to collect cluster metrics in addition to node metrics

=cut

has 'with_cluster_metrics' => (
    is      => 'lazy',
    isa     => Bool,
    builder => sub {
        my ($self) = @_;
        if( my $info = $self->node_details ) {
            return !!grep { $_ eq 'master' } @{ $info->{roles} };
        }
        return 0;
    },
);


=attr with_index_metrics

Boolean, set to true to collect index level metrics in addition to node metrics

=cut

has 'with_index_metrics' => (
    is      => 'lazy',
    isa     => Bool,
    builder => sub {
        my ($self) = @_;
        if( my $info = $self->node_details ) {
            return !!grep { /^data/ } @{ $info->{roles} };
        }
        return 0;
    },
);

=method get_metrics()

Retrieves the metrics from the local node.

=cut

sub get_metrics {
    my ($self) = @_;

    # Fetch Node Local Stats
    my @collected = $self->collect_node_metrics();

    push @collected, $self->collect_cluster_metrics()
        if $self->with_cluster_metrics;

    push @collected, $self->collect_index_metrics()
        if $self->with_index_metrics;

    # Flatten Collected and Return the Stats
    return \@collected;
}

=method collect_node_metrics()

Returns all relevant stats from the C<_nodes/_local> API

=cut

sub collect_node_metrics {
    my ($self) = @_;

    if( my $res = $self->request('_nodes/_local/stats')->content ) {
        return $self->_stat_collector( $res->{nodes}{$self->node_id} );
    }

    # Explicit return of empty list
    return;
}

=method collect_cluster_metrics()

Return all relevant stats from the C<_cluster/health> API as well as a count of
`index.blocks.*` in place.

=cut

sub collect_cluster_metrics {
    my ($self) = @_;

    my @stats = ();

    if( my $res = $self->request('_cluster/health')->content ) {
        push @stats,
            { key => "cluster.nodes.total",         value => $res->{number_of_nodes},       },
            { key => "cluster.nodes.data",          value => $res->{number_of_data_nodes},  },
            { key => "cluster.shards.primary",      value => $res->{active_primary_shards}, },
            { key => "cluster.shards.active",       value => $res->{active_shards},         },
            { key => "cluster.shards.initializing", value => $res->{initializing_shards},   },
            { key => "cluster.shards.relocating",   value => $res->{relocating_shards},     },
            { key => "cluster.shards.unassigned",   value => $res->{unassigned_shards},     },
            ;
    }
    push @stats, $self->_collect_index_blocks();
    return @stats;
}

sub _collect_index_blocks {
    my ($self) = @_;

    my @req = (
        '_settings/index.blocks.*',
        {
            index => '_all',
            uri_param => {
                flat_settings => 'true',
            },
        },
    );

    if( my $res = $self->request(@req)->content ) {
        my %collected=();
        foreach my $idx ( keys %{ $res } ) {
            if( my $settings = $res->{$idx}{settings} ) {
                foreach my $block ( keys %{ $settings } ) {
                    my $value = $settings->{$block};
                    if( lc $value eq 'true') {
                        $collected{$block} ||= 0;
                        $collected{$block}++;
                    }
                }
            }
        }
        return map { { key => "cluster.$_", value => $collected{$_} } } sort keys %collected;
    }

    # Explicit return of empty list
    return;
}

=method collect_index_metrics()

This method totals the shard, and segment state and size for the current node by index base name.

=cut

sub collect_index_metrics {
    my ($self) = @_;

    my $id = $self->node_id;
    my $shardres = $self->request('_cat/shards',
        {
            uri_param => {
                local  => 'true',
                format => 'json',
                bytes  => 'b',
                h => join(',', qw( index prirep docs store id state )),
            }
        }
    )->content;

    my %results;
    foreach my $shard ( @{ $shardres } ) {
        # Skip unallocated shards
        next unless $shard->{id};

        # Skip unless this shard is allocated to this shard
        next unless $shard->{id} eq $id;

        # Skip "Special" Indexes
        next if $shard->{index} =~ /^\./;

        # Figure out the Index Basename
        my $index = $shard->{index} =~ s/[-_]\d{4}([.-])\d{2}\g{1}\d{2}(?:[-_.]\d+)?$//r;
        next unless $index;
        $index =~ s/\./_/g;

        my $type  = $shard->{prirep} eq 'p' ? 'primary' : 'replica';

        # Initialize
        $results{$index} ||=  { map { $_ => 0 } qw( docs bytes primary replica ) };
        $results{$index}->{state} ||= {};
        $results{$index}->{state}{$shard->{state}} ||= 0;
        $results{$index}->{state}{$shard->{state}}++;

        # Add it up, Add it up
        $results{$index}->{docs}  += $shard->{docs};
        $results{$index}->{bytes} += $shard->{store};
        $results{$index}->{$type}++;
    }

    my @results;
    foreach my $idx (sort keys %results) {
        foreach my $k ( sort keys %{ $results{$idx} } ) {
            # Skip the complex
            next if ref $results{$idx}->{$k};
            push @results,
                {
                    key => sprintf("node.indices.%s.%s", $idx, $k),
                    value => $results{$idx}->{$k},
                };
        }
        my $states = $results{$idx}->{state} || {};

        foreach my $k ( sort keys %{ $states } ) {
            push @results,
                {
                    key => sprintf("node.indices.%s.state.%s", $idx, $k),
                    value => $states->{$k},
                };
        }
    }
    return @results;
}

#------------------------------------------------------------------------#
# Parse Statistics Dynamically
sub _stat_collector {
    my $self  = shift;
    my $ref   = shift;
    my @path  = @_;
    my @stats = ();

    # Base Case
    return unless is_hashref($ref);

    my %ignores = map { $_ => 1 } @{ $self->ignore }, @_IGNORES;
    foreach my $key (sort keys %{ $ref }) {
        # Skip uninteresting keys
        next if $ignores{$key};

        # Skip peak values, we'll see those in the graphs.
        next if $key =~ /^peak/;

        # Sanitize Key Name
        my $key_name = $key;
        $key_name =~ s/(?:time_)?in_millis/ms/;
        $key_name =~ s/(?:size_)?in_bytes/bytes/;
        $key_name =~ s/\./_/g;

        if( is_hashref($ref->{$key}) ) {
            # Recurse
            push @stats, $self->_stat_collector($ref->{$key},@path,$key_name);
        }
        elsif( $ref->{$key} =~ /^\d+(?:\.\d+)?$/ ) {
            # Numeric
            push @stats, {
                key   => join('.',@path,$key_name),
                value => $ref->{$key},
            };
        }
    }

    return @stats;
}

__PACKAGE__->meta->make_immutable;
