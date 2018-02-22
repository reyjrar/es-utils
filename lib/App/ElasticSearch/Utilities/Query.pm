package App::ElasticSearch::Utilities::Query;
# ABSTRACT: Object representing ES Queries

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use Clone qw(clone);
use Moo;
use Ref::Util qw(is_arrayref is_hashref);
use Types::Standard qw(ArrayRef HashRef Int Maybe Str);
use Types::ElasticSearch qw(TimeConstant is_TimeConstant);
use namespace::autoclean;

my %TO = (
    array_ref => sub { defined $_[0] && ref($_[0]) eq 'ARRAY' ? $_[0] : defined $_[0] ? [ $_[0] ] : $_[0] },
);

=attr query_stash

Hash reference containing replaceable query elements.  See L<stash>.

=cut

has query_stash => (
    is       => 'rw',
    isa      => HashRef,
    lazy     => 1,
    init_arg => undef,
    default  => sub {{}},
);

=attr must

The must section of a bool query as an array reference.  See: L<add_bool>
Can be set using set_must and is a valid init_arg.

=attr must_not

The must_not section of a bool query as an array reference.  See: L<add_bool>
Can be set using set_must_not and is a valid init_arg.

=attr should

The should section of a bool query as an array reference.  See: L<add_bool>
Can be set using set_should and is a valid init_arg.

=attr filter

The filter section of a bool query as an array reference.  See: L<add_bool>
Can be set using set_filter and is a valid init_arg.

=attr nested

The nested query, this shortcircuits the rest of the query due to restrictions
on the nested queries.

=attr nested_path

The path by being nested, only used in nested queries.

=cut

my %QUERY = (
    must        => { default => sub {undef}, isa => Maybe[ArrayRef], coerce => $TO{array_ref}, init_arg => 'must'     },
    must_not    => { default => sub {undef}, isa => Maybe[ArrayRef], coerce => $TO{array_ref}, init_arg => 'must_not' },
    should      => { default => sub {undef}, isa => Maybe[ArrayRef], coerce => $TO{array_ref}, init_arg => 'should'   },
    filter      => { default => sub {undef}, isa => Maybe[ArrayRef], coerce => $TO{array_ref}, init_arg => 'filter'   },
    nested      => { default => sub {undef}, isa => Maybe[HashRef], init_arg => 'nested' },
    nested_path => { default => sub {undef}, isa => Maybe[Str],     init_arg => 'nested_path' },
);

=attr from

Integer representing the offset the query should start returning documents from.  The default is undefined, which
falls back on the Elasticsearch default of 0, or from the beginning.
Can be set with B<set_from>.  Cannot be an init_arg.

=attr size

The number of documents to return in the query.  The default size is B<50>.
Can be set with B<set_size>.  Cannot be an init_arg.

=attr fields

An array reference containing the names of the fields to retrieve with the query.  The default is undefined, which
falls back on the Elasticsearch default of empty, or no fields retrieved.  The B<_source> is still retrieved.
Can be set with B<set_fields>.  Cannot be an init_arg.

=attr sort

An array reference of sorting keys/directions.  The default is undefined, which falls back on the Elasticsearch
default of B<score:desc>.
Can be set with B<set_sort>.  Cannot be an init_arg.

=attr aggregations

A hash reference of aggergations to perform.  The default is undefined, which means do not perform any aggregations.
Can be set with B<set_aggregations>, which is aliased as B<set_aggs>.  Cannot be an init_arg.
Aliased as B<aggs>.

=cut

my %REQUEST_BODY = (
    from         => { isa => Maybe[Int] },
    size         => { default => sub {50},    isa => Int },
    fields       => { isa => Maybe[ArrayRef], coerce => $TO{array_ref} },
    sort         => { isa => Maybe[ArrayRef], coerce => $TO{array_ref} },
    aggregations => { isa => Maybe[HashRef] },
);

=attr scroll

An L<ElasticSearch time constant|https://www.elastic.co/guide/en/elasticsearch/reference/master/common-options.html#time-units>.
The default is undefined, which means scroll will not be set on a query.
Can be set with B<set_scroll>.  Cannot be an init_arg.
See also: L<set_scan_scroll>.

=attr timeout

An L<ElasticSearch time constant|https://www.elastic.co/guide/en/elasticsearch/reference/master/common-options.html#time-units>.
The default is undefined, which means it will default to the connection timeout.
Can be set with B<set_timeout>.  Cannot be an init_arg.

=attr terminate_after

The number of documents to cancel the search after.  This generally shouldn't be used except for
large queries where you are protecting against OOM Errors. The B<size> attribute is more accurate as it's
truncation occurs after the reduce operation, where B<terminate_after> occurs during the map phase of the query.
Can be set with B<set_terminateafter>.  Cannot be an init_arg.

=cut

my %PARAMS = (
    scroll          => { isa => Maybe[TimeConstant] },
    timeout         => { isa => TimeConstant },
    terminate_after => { isa => Int },
);

# Dynamically build our attributes
foreach my $attr (keys %QUERY) {
    has $attr => (
        is       => 'rw',
        lazy     => 1,
        writer   => "set_$attr",
        init_arg => undef,
        %{ $QUERY{$attr} },
    );
}
foreach my $attr (keys %REQUEST_BODY) {
    has $attr => (
        is       => 'rw',
        lazy     => 1,
        writer   => "set_$attr",
        init_arg => undef,
        %{ $REQUEST_BODY{$attr} },
    );
}
foreach my $attr (keys %PARAMS) {
    has $attr => (
        is       => 'rw',
        lazy     => 1,
        writer   => "set_$attr",
        init_arg => undef,
        %{ $PARAMS{$attr} },
    );
}

=method uri_params()

Retrieves the URI parameters for the query as a hash reference.  Undefined parameters
will not be represented in the hash.

=cut

sub uri_params {
    my $self = shift;

    my %params=();
    foreach my $field (keys %PARAMS) {
        my $v = eval {
            debug({color=>'magenta'}, "uri_params() - retrieving param '$field'");
            ## no critic
            no strict 'refs';
            $self->$field();
            ## user critic
        };
        next unless defined $v;
        $params{$field} = $v;
    }
    return \%params;
}

=method request_body()

Builds and returns a hash reference representing the request body for the
Elasticsearch query.  Undefined elements will not be represented in the hash.

=cut

sub request_body {
    my $self = shift;

    my %body = ();
    foreach my $section (keys %REQUEST_BODY) {
        eval {
            debug({color=>'yellow'}, "request_body() - retrieving section '$section'");
            ## no critic
            no strict 'refs';
            $body{$section} = $self->$section;
            ## use critic
            delete $body{$section} unless defined $body{$section};
            debug_var({color=>'cyan'},$body{$section}) if defined $body{$section} and ref $body{$section};
            1;
        } or do {
            debug({color=>'red'}, "request_body() - Failed to retrieve '$section'");
        };
    }
    $body{query} = $self->query;
    return \%body;
}

=method query()

Builds and returns a hash reference represnting the bool query section of the
request body.  This function is called by the L<request_body> function but is
useful and distinct enough to expose as it's own method.  Undefined elements of
the query will not be represented in the hash it returns.

=cut

sub query {
    my $self = shift;

    my $qref;
    if( $self->nested ) {
        $qref = {
            nested => {
                path  => $self->nested_path,
                query => $self->nested,
            }
        };
    }
    else {
        my %bool = ();
        foreach my $k (keys %QUERY) {
            next if $k =~ /^nested/;
            $bool{$k} = [];
            my $v;
            eval {
                debug({color=>'yellow'}, "query() - retrieving section '$k'");
                ## no critic
                no strict 'refs';
                $v = $self->$k();
                ## user critic
                debug_var({color=>'cyan'},$v) if defined $v and ref $v;
                1;
            } or do {
                debug({color=>'red'}, "query() - Failed to retrieve '$k'");
            };
            $bool{$k} = clone $v if defined $v;
            if($self->stash($k)) {
                push @{ $bool{$k} }, $self->stash($k);
            }
            delete $bool{$k} if exists $bool{$k} and not @{ $bool{$k} };
        }
        $qref = { bool => \%bool };
    }
    return $qref;
}

=method add_aggregations( name => { ...  } )

Takes one or more key-value pairs.  The key is the name of the aggregation.
The value being the hash reference representation of the aggregation itself.
It will silently replace a previously named aggregation with the most recent
call.

Calling this function overrides the L<size> element to B<0> and L<scroll> to undef.

Aliased as B<add_aggs>.

=cut

sub add_aggregations {
    my $self = shift;
    my %aggs = @_;

    my $aggs = $self->aggregations();
    $aggs ||= {};
    foreach my $agg (keys %aggs) {
        debug("aggregation[$agg] added to query");
        $aggs->{$agg} = $aggs{$agg};
    }
    $self->set_aggregations($aggs);
    $self->set_size(0);
    $self->set_scroll(undef);
}

=method wrap_aggregations( name => { ... } )

Use this to wrap an aggregation in another aggregation.  For example:

    $q->add_aggregation(ip => { terms => { field => src_ip } });

Creates:

    {
        "aggs": {
            "ip": {
                "terms": {
                    "field": "src_ip"
                }
            }
        }
    }


Would give you the top IP for the whole query set.  To wrap that aggregation to get top IPs per hour, you could:

    $q->wrap_aggregations( hourly => { date_histogram => { field => 'timestamp', interval => '1h' } } );

Which translates the query into:

    {
        "aggs": {
            "hourly": {
                "date_histogram": {
                    "field": "timestamp",
                    "interval": "1h"
                }
                "aggs": {
                    "ip": {
                        "terms": {
                            "field": "src_ip"
                        }
                    }
                }
            }
        }
    }

=cut

sub wrap_aggregations {
    my $self = shift;
    my %wrapper = @_;
    foreach my $a (keys %wrapper) {
        $wrapper{$a}->{aggs} = clone $self->aggregations;
    }
    $self->set_aggregations(\%wrapper);
}

# Support Short-hand like ES
*aggs      = \&aggregations;
*set_aggs  = \&set_aggregations;
*add_aggs  = \&add_aggregations;
*wrap_aggs = \&wrap_aggregations;

=for Pod::Coverage aggs
=for Pod::Coverage set_aggs
=for Pod::Coverage add_aggs
=for Pod::Coverage wrap_aggs
=cut


=method set_scan_scroll($ctxt_life)

This function emulates the old scan scroll feature in early version of Elasticsearch. It takes
an optional  L<ElasticSearch time constant|https://www.elastic.co/guide/en/elasticsearch/reference/master/common-options.html#time-units>,
but defaults to '1m'.  It is the same as calling:

    $self->set_sort( [qw(_doc)] );
    $self->set_scroll( $ctxt_life );

=cut

sub set_scan_scroll {
    my ($self,$ctxt_life) = @_;

    # Validate Context Lifetime
    if( !is_TimeConstant( $ctxt_life) ) {
        undef($ctxt_life);
    }
    $ctxt_life ||= '1m';

    $self->set_sort( [qw(_doc)] );
    $self->set_scroll( $ctxt_life );
    $self;
}

=method set_match_all()

This method clears all filters and query elements to and sets the must to match_all.
It will not reset other parameters like size, sort, and aggregations.

=cut
sub set_match_all {
    my ($self) = @_;
    # Reset the relevant pieces of the query
    $self->set_must_not([]);
    $self->set_filter([]);
    $self->set_should([]);
    # Set the match_all bits
    $self->set_must({match_all=>{}});
    $self;
}

=method add_bool( section => condition )

Appends a search condition to a section in the query body.  Valid query body
points are: must, must_not, should, and filter.

=cut

sub add_bool {
    my $self      = shift;
    my $section   = shift;
    my $condition = shift;

    if( exists $QUERY{$section} ) {
        ## no critic
        no strict 'refs';
        my $set = $self->$section;
        push @{ $set }, $condition;
        my $setter = "set_$section";
        $self->$setter($set);
        ## use critic
    }
    $self;
}

=method stash( section => condition )

Allows a replaceable query element to exist in the query body sections: must, must_not,
should, and/or filter.  This is useful for moving through a data-set preserving everthing in a query
except one piece that shifts.  Imagine:

    my $query = App::ElasticSearch::Utilities::Query->new();
    $query->add_bool(must => { terms => {src_ip => [qw(1.2.3.4)]} });
    $query->add_bool(must => { range => { attack_score => { gt => 10 }} });

    while( 1 ) {
        $query->stash( must => { range => { timestamp => { gt => now() } } } );
        my @results = make_es_request( $query->request_body, $query->uri_params );

        # Long processing
    }

This allows re-use of the query object inside of loops like this.

=cut

sub stash {
    my ($self,$section,$condition) = @_;

    my $stash = $self->query_stash;
    if( exists $QUERY{$section} ) {
        if( defined $condition ) {
            debug({color=>exists $stash->{$section} ? 'green' : 'red' }, "setting $section in stash");
            $stash->{$section} = $condition;
        }
    }
    return exists $stash->{$section} ? $stash->{$section} : undef;
}

# Return True
1;
