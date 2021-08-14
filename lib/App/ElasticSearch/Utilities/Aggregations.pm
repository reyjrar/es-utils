package App::ElasticSearch::Utilities::Aggregations;
# ABSTRACT: Code to simplify creating and working with Elasticsearh aggregations

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

my %Aggregations = (
    avg => { single_stat => 1 },
    cardinality => { single_stat => 1 },
    date_histogram => {
        params => sub { { calendar_interval => $_[0] || '1h' } },
    },
    extend_stats => {},
    geo_centroid => {},
    geohash_grid => {
        params => sub { $_[0] =~ /^\d+$/ ? { precision => $_[0] } : {} },
    },
    histogram => {
        params => sub {
            return unless $_[0] > 0;
            return { interval => $_[0] };
        },
    },
    max => { single_stat => 1 },
    min => { single_stat => 1 },
    missing => { single_stat => 1 },
    percentiles => {
        params => sub {
            my @pcts = $_[0] ? split /,/, $_[0] : qw(25 50 75 90);
            return { percents => \@pcts };
        },
    },
    rare_terms => {
        params => sub { $_[0] =~ /^\d+$/ ? { max_doc_count => $_[0] } : {} },
    },
    terms => {
        params => sub { $_[0] =~ /^\d+$/ ? { size => $_[0] } : {} },
    },
    significant_terms => {
        params => sub { $_[0] =~ /^\d+$/ ? { size => $_[0] } : {} },
    },
    stats => {},
    sum => { single_stat => 1 },
    weighted_avg => {},
);

=func is_single_stat()

Returns true if an aggregation returns a single value.

=cut

sub is_single_stat {
    my ($agg) = @_;
    return unless $agg;
    return unless exists $Aggregations{$agg};
    return unless exists $Aggregations{$agg}{single_stat};
    return $Aggregations{$agg}{single_stat};
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
        $alias ||= join "_", $agg eq 'terms' ? ($field) : ($agg, $field);
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
            push @{ $row }, $field, delete $result->{key};
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
