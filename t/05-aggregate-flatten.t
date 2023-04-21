#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities::Aggregations;
use CLI::Helpers qw(:output);
use Data::Dumper;
use Test::More;
use YAML::XS qw();

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;

# Aggregation Flattening Tests
my $tests = YAML::XS::Load(join '', <DATA>);

foreach my $t (sort keys %{ $tests }) {
    my $flat = es_flatten_aggs( $tests->{$t}{aggregations} );

    is_deeply( $flat, $tests->{$t}{expected}, $t )
        or diag( Dumper $flat );
}
done_testing();

__DATA__
---
00-simple_terms_agg:
  aggregations:
    ip:
      buckets:
        - key: 1.2.3.4
          doc_count: 13
  expected:
    -
      - ip
      - 1.2.3.4
      - ip.hits
      - 13
01-simple_embedded_terms:
  aggregations:
    ip:
      buckets:
        - key: 1.2.3.4
          doc_count: 13
          ports:
            buckets:
              - key: 53
                doc_count: 13
  expected:
    -
      - ip
      - 1.2.3.4
      - ip.hits
      - 13
      - ports
      - 53
      - ports.hits
      - 13
00-simple_terms_agg_key_as_string:
  aggregations:
    ip:
      buckets:
        - key: 1.2.3.4
          key_as_string: "one dot two dot three dot four"
          doc_count: 13
  expected:
    -
      - ip
      - one dot two dot three dot four
      - ip.raw
      - 1.2.3.4
      - ip.hits
      - 13
