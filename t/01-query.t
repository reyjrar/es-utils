#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities::Query;
use CLI::Helpers qw(:output);
use Data::Dumper;
use Test::More;

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;


my $q = App::ElasticSearch::Utilities::Query->new();

# Add a single boolean
$q->add_bool( must => { term => { foo => 'bar' } } );
my $expected = {
   'bool' => {
     'must' => [
       {
         'term' => {
           'foo' => 'bar'
         }
       }
     ]
   }
};
check_query('simple must');

# Add a second parameter
$q->add_bool( must => { term => { bar => 'baz' } } );
push @{ $expected->{bool}{must} }, { term => { bar => 'baz' } };
check_query('second must parameter');

my $should = [
    { term => { a => 'b' } },
    { term => { c => 'd' } }
];
$q->add_bool( should => $_ ) for @{ $should };
$expected->{bool}{should} = $should;
check_query('should parameters');

# Set minimum should match
$q->minimum_should_match(1);
$expected->{bool}{minimum_should_match} = 1;
check_query('minimum_should_match');

done_testing();

sub check_query {
    my ($name) = @_;
    is_deeply( $q->query, $expected, $name)
        or diag( Dumper $q->query );
}
