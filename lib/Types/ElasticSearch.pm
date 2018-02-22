package Types::ElasticSearch;
# ABSTRACT: Types for working with ElasticSearch

use Type::Library -base;
use Type::Tiny;

my $TimeConstant = Type::Tiny->new(
    name       => "TimeConstant",
    constraint => sub { defined($_) && /^\d+(y|M|w|d|h|m|s|ms)$/ },
    message    => sub {
        "must be time constant: https://www.elastic.co/guide/en/elasticsearch/reference/master/common-options.html#time-units"
    },
);

__PACKAGE__->meta->add_type($TimeConstant);
__PACKAGE__->meta->make_immutable;
