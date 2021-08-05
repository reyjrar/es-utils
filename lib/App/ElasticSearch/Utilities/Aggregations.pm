package App::ElasticSearch::Utilities::Aggregations;
# ABSTRACT: Code to simplify creating and working with Elasticsearh aggregations

use strict;
use warnings;

use Sub::Exporter -setup => {
    exports => [ qw(
        expand_aggregate_string
        is_single_stat
    )],
    groups => {
        default => [qw(expand_aggregate_string is_single_stat)],
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

=cut

sub is_single_stat {
    my ($agg) = @_;
    return unless $agg;
    return unless exists $Aggregations{$agg};
    return unless exists $Aggregations{$agg}{single_stat};
    return $Aggregations{$agg}{single_stat};
}


=func expand_aggregate_string( token )

=cut

sub expand_aggregate_string {
    my ($token) = @_;

    my %aggs = ();
    foreach my $def ( split /\+/, $token ) {
        my $alias = $def =~ s/^(\w+)=// ? $1 : undef;
        my @parts = split /:/, $def, 3;
        if( @parts == 1 ) {
            $aggs{$def} = { terms => { field => $def, size => 20 } };
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

1;
