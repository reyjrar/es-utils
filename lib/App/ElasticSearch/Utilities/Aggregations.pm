package App::ElasticSearch::Utilities::Aggregations;
# ABSTRACT: Code to simplify creating and working with Elasticsearch aggregations

use strict;
use warnings;

use Storable qw(dclone);
use Sub::Exporter -setup => {
    exports => [ qw(
        expand_aggregate_string
        es_flatten_aggregations es_flatten_aggs
        is_single_stat
    )],
    groups => {
        default => [qw(
            expand_aggregate_string
            es_flatten_aggregations es_flatten_aggs
            is_single_stat
        )],
    },
};

my %Aggregations;

=head1 Aggregations

List of supported aggregations.  Other aggregation may work, but these have defined behavior.

=head2 Bucket Aggregations

These aggregations will support sub aggregations.

=over 2

=cut

$Aggregations{terms} = {
    params    => sub { $_[0] && $_[0] =~ /^\d+$/ ? { size => $_[0] } : {} },
    type      => 'bucket',
    composite => 1,
};

=item B<terms>

The default aggregation if none is specified.

    field_name
    terms:field_name

Results in

    {
        "field_name": {
            "terms": {
                "field": "field_name"
            }
        }
    }

Supports a positional parameter: size

    field_name:20
    terms:field_name:20

Results in

    {
        "field_name": {
            "terms": {
                "field": "field_name",
                "size": 20
            }
        }
    }

=cut

$Aggregations{significant_terms} = {
    params => sub { $_[0] =~ /^\d+$/ ? { size => $_[0] } : {} },
    type   => 'bucket',
};

=item B<significant_terms>

Same as C<terms>.

    significant_terms:field_name:10

Results in:

    {
        "rare_terms.field_name": {
            "terms": {
                "field": "field_name",
                "size": 10
            }
        }
    }

=cut

$Aggregations{rare_terms} = {
    params => sub { $_[0] =~ /^\d+$/ ? { max_doc_count => $_[0] } : {} },
    type   => 'bucket',
};

=item B<rare_terms>

Same as C<terms> but the positional parameter is the C<max_doc_count>.

    rare_terms:field_name:10

Results in:

    {
        "rare_terms.field_name": {
            "terms": {
                "field": "field_name",
                "max_doc_count": 10
            }
        }
    }

=cut

$Aggregations{histogram} = {
    params => sub {
        return unless $_[0] > 0;
        return { interval => $_[0] };
    },
    type      => 'bucket',
    composite => 1,
};

=item B<histogram>

Creates a histogram for numeric fields.  Positional parameter is the interval.

    histogram:field_name:10

Results in:

    {
        "histogram.field_name": {
            "histogram": {
                "field": "field_name",
                "interval": 10
            }
        }
    }

=cut

$Aggregations{date_histogram} = {
    params    => sub { { calendar_interval => $_[0] || '1h' } },
    type      => 'bucket',
    composite => 1,
};

=item B<date_histogram>

Creates a histogram for date fields.  Positional parameter is the calendar_interval.

    date_histogram:field_name:1h

Results in:

    {
        "histogram.field_name": {
            "histogram": {
                "field": "field_name",
                "calendar_interval": "1h"
            }
        }
    }

=cut

$Aggregations{geohash_grid} = {
    params    => sub { $_[0] =~ /^\d+$/ ? { precision => $_[0] } : {} },
    type      => 'bucket',
    composite => 1,
};

=item B<geohash_grid>

Creates a geohash grid bucket aggregation.  Positional parameter is the precision.

    geohash_grid:field_name:6

Results in:

    {
        "geohash_grid.field_name": {
            "geohash_grid": {
                "field": "field_name",
                "precision": 6
            }
        }
    }

=cut

$Aggregations{missing} = { type => 'bucket' };

=item B<missing>

Creates a bucket for documents missing the field.  No positional parameters.

    missing:field_name

Results in:

    {
        "missing.field_name": {
            "missing": {
                "field": "field_name"
            }
        }
    }

=back

=head2 Metric Aggregations

Aggregations that generate metrics from enclosing buckets.

=over 2

=cut

$Aggregations{avg} = { single_stat => 1, type => 'metric' };
$Aggregations{max} = { single_stat => 1, type => 'metric' };
$Aggregations{min} = { single_stat => 1, type => 'metric' };
$Aggregations{sum} = { single_stat => 1, type => 'metric' };

=item B<avg>, B<max>, B<min>, B<sum>

Single stat metric aggregations to generate the various single statistics over the enclosing bucket.

    sum:field_name

Results in

    {
        "sum.field_names": {
            "sum": {
                "field": "field_name"
            }
        }
    }

=cut

$Aggregations{cardinality} = { single_stat => 1, type => 'metric' };

=item B<cardinality>

Computes the unique count of terms in a field.

    cardinality:field_name

Results in

    {
        "cardinality.field_names": {
            "cardinality": {
                "field": "field_name"
            }
        }
    }

=cut

$Aggregations{stats} = { type => 'metric' };

=item B<stats>

Runs the stats aggregation that returns min, max, avg, sum, and count.

    stats:field_name

Results in

    {
        "stats.field_names": {
            "stats": {
                "field": "field_name"
            }
        }
    }

=cut

$Aggregations{extended_stats} = { type => 'metric' };

=item B<extended_stats>

Runs the stats aggregation that returns the same data as the C<sum> aggregation
plus variance, sum of squares, and standard deviation.

    extended_stats:field_name

Results in

    {
        "extended_stats.field_names": {
            "extended_stats": {
                "field": "field_name"
            }
        }
    }

=cut

$Aggregations{percentiles} = {
    params => sub {
        my @pcts = $_[0] ? split /,/, $_[0] : qw(25 50 75 90);
        return { percents => \@pcts };
    },
};

=item B<percentiles>

Computes percentiles for the enclosing bucket. The positional parameter is
interpretted at the percents computed.  If ommitted, the percentiles computed
will be: 25, 50, 75, 90.

    percentiles:field_name:75,95,99

Results in

    {
        "percentiles.field_names": {
            "percentiles": {
                "field": "field_name",
                "percents": [ 75, 95, 99 ]
            }
        }
    }

=cut

$Aggregations{geo_centroid} = { type => 'metric' };


=item B<geo_centroid>

Computes center of a group of geo points. No positional parameters supported.

    geo_centroid:field_name

Results in

    {
        "geo_centroid.field_names": {
            "geo_centroid": {
                "field": "field_name"
            }
        }
    }

=cut

=back

=func is_single_stat()

Returns true if an aggregation returns a single value.

=cut

sub is_single_stat {
    my ($agg) = @_;
    return unless $agg;
    return unless exists $Aggregations{$agg};
    return unless exists $Aggregations{$agg}->{single_stat};
    return $Aggregations{$agg}->{single_stat};
}

=func expand_aggregate_string( token )

Takes a simplified aggregation grammar and expands it the full aggregation hash.

Simple Terms:

    field_name

To

    {
        field_name => {
            terms => {
                field => 'field_name',
                size  => 20,
            }
        }
    }

Alias expansion:

    alias=field_name

To

    {
        alias => {
            terms => {
                field => 'field_name',
                size  => 20,
            }
        }
    }

Parameters:

    alias=field_name:10

To

    {
        alias => {
            terms => {
                field => 'field_name',
                size  => 10,
            }
        }
    }

Parameters, k/v:

    alias=field_name:size=13

To

    {
        alias => {
            terms => {
                field => 'field_name',
                size  => 13,
            }
        }
    }

=cut

sub expand_aggregate_string {
    my ($token) = @_;

    my %aggs = ();
    foreach my $def ( split /\+/, $token ) {
        my $alias = $def =~ s/^(\w+)=// ? $1 : undef;
        my @parts = split /:/, $def, 3;
        if( @parts == 1 ) {
            $alias ||= $def;
            $aggs{$alias} = { terms => { field => $def, size => 20 } };
            next;
        }
        my ($agg, $field);
        if( exists $Aggregations{$parts[0]} ) {
            $agg     = shift @parts;
            $field   = shift @parts;
        }
        else {
            $agg = 'terms';
            $field = shift @parts;
        }
        my $params  = {};
        my $paramStr = shift @parts;

        if( $paramStr && $paramStr =~ /\w+=/ ) {
            # split on commas using a positive lookahead for a "word="
            foreach my $token (split /,(?=\w+=)/, $paramStr) {
                my ($k,$v) = split /=/, $token, 2;
                next unless $k and $v;
                $params->{$k} = $v =~ /,/ ? [ split /,/, $v ] : $v;
            }
        }
        elsif( exists $Aggregations{$agg}->{params} ) {
            # Process parameters
            $params = $Aggregations{$agg}->{params}->($paramStr);
        }
        $alias ||= join ".", $agg eq 'terms' ? ($field) : ($agg, $field);
        $aggs{$alias} = { $agg => { field => $field, %{ $params } } };
    }
    return \%aggs;
}

=func es_flatten_aggregations()

Takes the B<aggregations> section of the query result and parses it into a flat
structure so each row contains all the sub aggregation information.

It returns an array reference, containing arrray references.  The individual
rows of the array are ordered in a depth first fashion.  The array does include
a key for every value, so the array can be cast to a hash safely.

=cut

sub es_flatten_aggregations {
    my ($result,$field,$parent) = @_;

    $parent ||= [];
    my @rows = ();

    my @remove = qw(
        doc_count_error_upper_bound
        sum_other_doc_count
    );

    my $row = dclone($parent);
    my $extract = sub {
        my ($key, $hash) = @_;

        if( $hash->{value} ) {
            push @{ $row }, $key, $hash->{value};
        }
        elsif( $hash->{values} ) {
            foreach my $k ( sort keys %{ $hash->{values} } ) {
                push @{ $row }, "$key.$k", $hash->{values}{$k}
                    if $hash->{values}{$k};
            }
        }
        else {
            foreach my $k (sort keys %{ $hash }) {
                last if $k eq 'buckets';
                push @{ $row }, "$key.$k", $hash->{$k}
                    if defined $hash->{values}{$k};
            }
        }
    };

    if( $field ) {
        delete $result->{$_} for @remove;
        if( $result->{key} and exists $result->{doc_count} ) {
            my $k = delete $result->{key};
            my $ks = delete $result->{key_as_string};
            push @{ $row }, $field, $ks || $k;
            push @{ $row }, "$field.hits", delete $result->{doc_count} || 0;
        }
        my %buckets = ();
        foreach my $k ( sort keys %{ $result } ) {
            if( ref $result->{$k} eq 'HASH' ) {
                $extract->($k, $result->{$k});

                if( my $buckets = delete $result->{$k}{buckets} ) {
                    $buckets{$k} = $buckets;
                }
            }
        }
        if( keys %buckets ) {
            foreach my $k ( sort keys %buckets ) {
                if( @{ $buckets{$k} } ) {
                    foreach my $bucket ( @{ $buckets{$k} } ) {
                        push @rows, @{ es_flatten_aggregations($bucket, $k, $row) };
                    }
                }
                else {
                    push @rows, $row;
                }
            }
        }
        else {
            push @rows, $row;
        }
    }
    else {
        foreach my $k ( sort keys %{ $result } ) {
            delete $result->{$k}{$_} for @remove;
            $extract->($k, $result->{$k});
            my $buckets = delete $result->{$k}{buckets};
            if( $buckets and @{ $buckets } ) {
                foreach my $bucket ( @{ $buckets } ) {
                    push @rows, @{ es_flatten_aggregations($bucket,$k,$row) };
                }
            }
            else {
                push @rows, $row;
            }
        }
    }

    return \@rows;
}

# Setup Aliases
*es_flatten_aggs = \&es_flatten_aggregations;

=for Pod::Coverage es_flatten_aggs

=cut

1;
