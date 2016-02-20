package App::ElasticSearch::Utilities::Query;
# ABSTRACT: Object representing ES Queries

use strict;
use warnings;

use CLI::Helpers qw(:output);
use Clone qw(clone);
use Moo;
use namespace::autoclean;
use Sub::Quote;

my %VALID = (
    array_ref     => quote_sub(q{die "must be an array reference" if defined $_[0] and ref $_[0] ne 'ARRAY'}),
    hash_ref      => quote_sub(q{die "must be a hash reference" if defined $_[0] and ref $_[0] ne 'HASH'}),
    time_constant => quote_sub(q{die "must be time constant: https://www.elastic.co/guide/en/elasticsearch/reference/master/common-options.html#time-units" if defined $_[0] && $_[0] !~ /^\d+(y|M|w|d|h|m|s|ms)$/ }),
    integer       => quote_sub(q{die "must be 0+ and integer" if defined $_[0] and $_[0] !~ /^\d+$/ }),
);
my %TO = (
    array_ref => quote_sub(q{defined $_[0] && ref $_[0] eq 'ARRAY' ? $_[0] : defined $_[0] ? [ $_[0] ] : $_[0]}),
);

=attr query_stash

Hash reference containing replaceable query elements.  See L<stash>.

=cut

has query_stash => (
    is       => 'rw',
    lazy     => 1,
    init_arg => undef,
    default  => sub {{}},
    isa      => $VALID{hash_ref},
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

=cut

my %QUERY = (
    must     => { default => sub {undef}, isa => $VALID{array_ref}, coerce => $TO{array_ref}, init_arg => 'must'     },
    must_not => { default => sub {undef}, isa => $VALID{array_ref}, coerce => $TO{array_ref}, init_arg => 'must_not' },
    should   => { default => sub {undef}, isa => $VALID{array_ref}, coerce => $TO{array_ref}, init_arg => 'should'   },
    filter   => { default => sub {undef}, isa => $VALID{array_ref}, coerce => $TO{array_ref}, init_arg => 'filter'   },
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
    from         => { default => sub {undef}, isa => $VALID{integer} },
    size         => { default => sub {50},    isa => $VALID{integer} },
    fields       => { default => sub {undef}, isa => $VALID{array_ref}, coerce => $TO{array_ref} },
    sort         => { default => sub {undef}, isa => $VALID{array_ref}, coerce => $TO{array_ref} },
    aggregations => { default => sub {undef}, isa => $VALID{hash_ref} },
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
    scroll          => { default => sub {undef}, isa => $VALID{time_constant} },
    timeout         => { default => sub {undef}, isa => $VALID{time_constant} },
    terminate_after => { default => sub {undef}, isa => $VALID{integer} },
);

# Dynamically build our attributes
foreach my $attr (keys %QUERY) {
    has $attr => (
        is => 'rw',
        lazy => 1,
        writer => "set_$attr",
        init_arg => undef,
        %{ $QUERY{$attr} },
    );
}
foreach my $attr (keys %REQUEST_BODY) {
    has $attr => (
        is => 'rw',
        lazy => 1,
        writer => "set_$attr",
        init_arg => undef,
        %{ $REQUEST_BODY{$attr} },
    );
}
foreach my $attr (keys %PARAMS) {
    has $attr => (
        is => 'rw',
        lazy => 1,
        writer => "set_$attr",
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
            no strict 'refs';
            $self->$field();
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
        no strict 'refs';
        eval {
            debug({color=>'yellow'}, "request_body() - retrieving section '$section'");
            $body{$section} = $self->$section;
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

    my %bool = ();
    foreach my $k (keys %QUERY) {
        no strict 'refs';
        $bool{$k} = [];
        my $v;
        eval {
            debug({color=>'yellow'}, "query() - retrieving section '$k'");
            $v = $self->$k();
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
    return { bool => \%bool };
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
# Support Short-hand like ES
*aggs = \&aggregations;
*set_aggs = \&set_aggregations;
*add_aggs = \&add_aggregations;

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
    eval {
        $VALID{time_constant}->($ctxt_life);
    } or do {
        undef($ctxt_life);
    };
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
        no strict 'refs';
        my $set = $self->$section;
        push @{ $set }, $condition;
        my $setter = "set_$section";
        $self->$setter($set);
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
