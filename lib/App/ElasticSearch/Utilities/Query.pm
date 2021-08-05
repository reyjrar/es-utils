package App::ElasticSearch::Utilities::Query;
# ABSTRACT: Object representing ES Queries

use strict;
use warnings;

# VERSION

use App::ElasticSearch::Utilities qw(es_request es_indices);
use App::ElasticSearch::Utilities::Aggregations;
use CLI::Helpers qw(:output);
use Clone qw(clone);
use Const::Fast;
use Moo;
use Ref::Util qw(is_arrayref is_hashref);
use Types::Standard qw(ArrayRef Enum HashRef Int Maybe Str);
use Types::ElasticSearch qw(TimeConstant is_TimeConstant);
use namespace::autoclean;

const my $AGG_KEY => 'aggregations';
my %TO = (
    array_ref => sub { defined $_[0] && is_arrayref($_[0]) ? $_[0] : defined $_[0] ? [ $_[0] ] : $_[0] },
);

=attr fields_meta

A hash reference with the field data from L<App::ElasticSearch::Utilities::es_index_fields>.

=cut

has fields_meta => (
    is => 'rw',
    isa => HashRef,
    default => sub { {} },
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

=attr scroll_id

The scroll id for the last executed query.  You shouldn't mess with this
directly. It's best to use the L<execute()> and L<scroll_results()> methods.

=cut

has scroll_id => (
    is       => 'rw',
    isa      => Str,
    init_arg => undef,
    writer   => 'set_scroll_id',
    clearer  => 1,
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

=attr minimum_should_match

A string defining the minimum number of should conditions to qualify a match.
See L<https://www.elastic.co/guide/en/elasticsearch/reference/7.3/query-dsl-minimum-should-match.html>

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
    must        => { isa => ArrayRef, coerce => $TO{array_ref} },
    must_not    => { isa => ArrayRef, coerce => $TO{array_ref} },
    should      => { isa => ArrayRef, coerce => $TO{array_ref} },
    filter      => { isa => ArrayRef, coerce => $TO{array_ref} },
    nested      => { isa => HashRef },
    nested_path => { isa => Str },
    minimum_should_match => { isa => Str },
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
    from         => { isa => Int },
    size         => { default => sub {50},    isa => Int },
    fields       => { isa => ArrayRef, coerce => $TO{array_ref} },
    sort         => { isa => ArrayRef, coerce => $TO{array_ref} },
    aggregations => { isa => HashRef },
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

=attr track_total_hits

Should the query attempt to calculate the number of hits the query would match.
Defaults to C<true>.

=attr track_scores

Set to true to score every hit in the search results, set to false to not
report scores.  Defaults to unset, i.e., use the ElasticSearch default.

=attr rest_total_hits_as_int

In ElasticSearch 7.0, the total hits element became a hash reference with more
details.  Since most of the tooling relies on the old behavior, this defaults
to C<true>.

=attr search_type

Choose an execution path for the query.  This is null by default, but you can
set it to a valid `search_type` setting, see:
L<https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-search-type>

=cut

my %PARAMS = (
    scroll           => { isa => Maybe[TimeConstant] },
    timeout          => { isa => TimeConstant },
    terminate_after  => { isa => Int },
    track_total_hits => { isa => Enum[qw( true false )], default => sub { 'true' } },
    track_scores     => { isa => Enum[qw( true false )] },
    search_type      => { isa => Enum[qw( dfs_query_then_fetch query_then_fetch )] },
    rest_total_hits_as_int => { isa => Enum[qw( true false )], default => sub { 'true' } },
);

# Dynamically build our attributes
foreach my $attr (keys %QUERY) {
    has $attr => (
        is       => 'rw',
        lazy     => 1,
        writer   => "set_$attr",
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
        %{ $PARAMS{$attr} },
    );
}

=method as_search( [ 'index1', 'index2' ] )

Returns a list of parameters to pass directly to C<es_request()>.

=cut

sub as_search {
    my ($self,$indexes) = @_;
    return (
        _search => {
            index     => $indexes,
            uri_param => $self->uri_params,
            method    => 'POST',
        },
        $self->request_body,
    );
}

=method execute( [ $index1, $index2 ] )

Uses `es_request()` to return the result, stores any relevant scroll data.

=cut

sub execute {
    my($self,$indexes) = @_;

    # Default to context based indexes
    $indexes ||= es_indices();

    my $result = es_request( $self->as_search($indexes) );

    if( $result->{_scroll_id} ) {
        $self->set_scroll_id($result->{_scroll_id})
    }

    return $result;
}

=method scroll_results()

If a scroll has been set, this will construct and run the requisite scroll
search, otherwise it returns undef.

=cut

sub scroll_results {
    my($self) = @_;
    my $result;
    if( $self->scroll_id ) {
        $result = es_request( '_search/scroll',
                { method => 'POST' },
                {
                    scroll => $self->scroll,
                    scroll_id => $self->scroll_id,
                }
        );
        if( $result && $result->{_scroll_id} ) {
            $self->set_scroll_id($result->{_scroll_id})
        }
        else {
            $self->clear_scroll_id();
        }
    }
    return $result ? $result : ();
}

=method uri_params()

Retrieves the URI parameters for the query as a hash reference.  Undefined parameters
will not be represented in the hash.

=cut

sub uri_params {
    my $self = shift;

    my %params=();
    foreach my $field (keys %PARAMS) {
        my $v;
        eval {
            ## no critic
            no strict 'refs';
            $v = $self->$field();
            ## user critic
        };
        debug({color=>'magenta'}, sprintf "uri_params() - retrieving param '%s' got '%s'",
                $field, ( defined $v ? $v : '' ),
        );

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
    my %map  = qw( fields _source );
    foreach my $section (keys %REQUEST_BODY) {
        my $val;
        eval {
            ## no critic
            no strict 'refs';
            $val = $self->$section;
            ## use critic
            1;
        } or do {
            debug({color=>'red'}, "request_body() - Failed to retrieve '$section'");
        };
        next unless defined $val;
        my $data = { $section => $val };
        my $param = $map{$section} || $section;
        $body{$param} = $val;
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
            delete $bool{$k} if exists $bool{$k} and is_arrayref($bool{$k}) and not @{ $bool{$k} };
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

Calling this function overrides the L<size> element to B<0> and disables L<scroll>.

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

    $q->add_aggregations(ip => { terms => { field => src_ip } });

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
    my $aggs = $self->aggregations;

    if( keys %{ $aggs } ) {
        foreach my $a (keys %wrapper) {
            $wrapper{$a}->{$AGG_KEY} = clone $aggs;
        }
    }

    $self->set_aggregations(\%wrapper);
}

=method aggregations_by( [asc | desc] => aggregation_string )

Applies a sort to all aggregations at the current level based on the
aggregation string.

Aggregation strings are parsed with the
L<App::ElasticSearch::Utilities::Aggregations> C<expand_aggregate_string()>
functions.

Examples:

    $q->aggregations_by( desc => [ qw( sum:bytes ) ] );
    $q->aggregations_by( desc => [ qw( sum:bytes cardinality:user_agent ) ] );

=cut

sub aggregations_by {
    my ($self,$dir,$aggs) = @_;

    my @sort = ();
    my %aggs = ();
    foreach my $def (@{ $aggs }) {
        my ($name,$agg) = %{ expand_aggregate_string($def) };
        next unless is_single_stat(keys %{ $agg });
        $aggs{$name} = $agg;
        push @sort, { $name => $dir };
    }
    if( @sort ) {
        push @sort, { '_count' => 'desc' };

        my $ref_aggs = $self->aggregations;
        foreach my $name ( keys %{ $ref_aggs } ) {
            foreach my $k ( keys %{ $ref_aggs->{$name} } ) {
                next if $k eq $AGG_KEY;
                $ref_aggs->{$name}{$k}{order} = \@sort;
                foreach my $agg (keys %aggs) {
                    $ref_aggs->{$name}{$AGG_KEY}{$agg} = $aggs{$agg};
                }
            }
        }
        $self->set_aggregations( $ref_aggs );
    }
}

# Support Short-hand like ES
*aggs      = \&aggregations;
*set_aggs  = \&set_aggregations;
*add_aggs  = \&add_aggregations;
*wrap_aggs = \&wrap_aggregations;
*aggs_by   = \&aggregations_by;

=for Pod::Coverage aggs
=for Pod::Coverage set_aggs
=for Pod::Coverage add_aggs
=for Pod::Coverage wrap_aggs
=for Pod::Coverage aggs_by
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

=method add_bool( section => conditions .. )

Appends a search condition to a section in the query body.  Valid query body
points are: must, must_not, should, and filter.

    $q->add_bool( must => { term => { http_status => 200 } } );

    # or

    $q->add_bool(
        must => [
            { term => { http_method => 'GET' } }
            { term => { client_ip   => '10.10.10.1' } }
        ]
        must_not => { term => { http_status => 400 } },
    );

=cut

sub add_bool {
    my $self  = shift;
    my %bools = @_;
    foreach my $section ( sort keys %bools ) {
        next unless exists $QUERY{$section};
        ## no critic
        no strict 'refs';
        my $set = $self->$section;
        push @{ $set }, $bools{$section};
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
        $query->stash( must => { range => { timestamp => { gt => time() } } } );
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
            # Reset Scroll ID
            $self->clear_scroll_id();
        }
    }
    return exists $stash->{$section} ? $stash->{$section} : undef;
}

# Return True
1;
