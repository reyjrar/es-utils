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
    terms => {
        params => sub { $_[0] =~ /^\d+$/ ? { size => $_[0] } : {} },
    },
    significant_terms => {
        params => sub { $_[0] =~ /^\d+$/ ? { size => $_[0] } : {} },
    },
    cardinality => {
        single_stat => 1,
    },
    avg => {
        single_stat => 1,
    },
    weighted_avg => {},
    extend_stats => {},
    stats => {},
    min => { single_stat => 1 },
    max => { single_stat => 1 },
    sum => { single_stat => 1 },
    histogram => {
        params => sub {
            return unless $_[0] > 0;
            return { interval => $_[0] };
        },
    },
    percentiles => {
        params => sub {
            my @pcts = $_[0] ? split /,/, $_[0] : qw(25 50 75 90);
            return { percents => \@pcts };
        },
    },

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
    foreach my $def ( split ';', $token ) {
        my $alias = $def =~ s/\=([^=]+)$// ? $1 : undef;
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
        if( exists $Aggregations{$agg}->{params} ) {
            # Process parameters
            $params = $Aggregations{$agg}->{params}->(@parts);
        }
        $alias ||= join "_", $agg eq 'terms' ? ($field) : ($agg, $field);
        $aggs{$alias} = { $agg => { field => $field, %{ $params } } };
    }
    return \%aggs;
}

1;
