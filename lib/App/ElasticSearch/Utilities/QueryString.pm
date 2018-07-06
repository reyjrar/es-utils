package App::ElasticSearch::Utilities::QueryString;
# ABSTRACT: CLI query string fixer

use strict;
use warnings;

# VERSION

use App::ElasticSearch::Utilities qw(:config);
use App::ElasticSearch::Utilities::Query;
use CLI::Helpers qw(:output);
use Module::Pluggable::Object;
use Moo;
use Ref::Util qw(is_arrayref);
use Types::Standard qw(ArrayRef Enum);

use namespace::autoclean;

=head1 SYNOPSIS

This class provides a pluggable architecture to expand query strings on the
command-line into complex Elasticsearch queries.

=cut

my %JOINING  = map { $_ => 1 } qw( AND OR );
my %TRAILING = map { $_ => 1 } qw( AND OR NOT );

=attr context

Defaults to 'query', but can also be set to 'filter' so the elements will be
added to the 'must' or 'filter' parameter.

=cut

has 'context' => (
    is      => 'rw',
    isa     => Enum[qw(query filter)],
    lazy    => 1,
    default => sub { 'query' },
);

=attr search_path

An array reference of additional namespaces to search for loading the query string
processing plugins.  Example:

    $qs->search_path([qw(My::Company::QueryString)]);

This will search:

    App::ElasticSearch::Utilities::QueryString::*
    My::Company::QueryString::*

For query processing plugins.

=cut

has search_path => (
    is      => 'rw',
    isa     => ArrayRef,
    default => sub {[]},
);

=attr default_join

When fixing up the query string, if two tokens are found next to eachother
missing a joining token, join using this token.  Can be either C<AND> or C<OR>,
and defaults to C<AND>.

=cut

has default_join => (
    is      => 'rw',
    isa     => Enum[qw(AND OR)],
    default => sub { 'AND' },
);

=attr plugins

Array reference of ordered query string processing plugins, lazily assembled.

=cut

has plugins => (
    is      => 'ro',
    isa     => ArrayRef,
    builder => '_build_plugins',
    lazy    => 1,
);

=method expand_query_string(@tokens)

This function takes a list of tokens, often from the command line via @ARGV.  Uses
a plugin infrastructure to allow customization.

Returns: L<App::ElasticSearch::Utilities::Query> object

=cut

sub expand_query_string {
    my $self = shift;

    my $query  = App::ElasticSearch::Utilities::Query->new();
    my @processed = ();
    TOKEN: foreach my $token (@_) {
        foreach my $p (@{ $self->plugins }) {
            my $res = $p->handle_token($token);
            if( defined $res ) {
                push @processed, is_arrayref($res) ? @{$res} : $res;
                next TOKEN;
            }
        }
        push @processed, { query_string => $token };
    }

    debug({color=>"magenta"}, "Processed parts");
    debug_var({color=>"magenta"},\@processed);

    my $context = $self->context eq 'query' ? 'must' : 'filter';
    my $invert=0;
    my @dangling=();
    my @qs=();
    foreach my $part (@processed) {
        if( exists $part->{dangles} ) {
            push @dangling, $part->{query_string};
        }
        elsif( exists $part->{query_string} ) {
            push @qs, @dangling, $part->{query_string};
            @dangling=(),
        }
        elsif( exists $part->{condition} ) {
            my $target = $invert ? 'must_not' : $context;
            $query->add_bool( $target => $part->{condition} );
            @dangling=();
        }
        elsif( exists $part->{nested} ) {
            $query->nested($part->{nested}{query});
            $query->nested_path($part->{nested}{path});
            @dangling=();
        }
        # Carry over the Inversion for instance where we jump out of the QS
        $invert = exists $part->{invert} && $part->{invert};
    }
    if(@qs)  {
        pop   @qs while @qs && exists $TRAILING{$qs[-1]};
        shift @qs while @qs && exists $JOINING{$qs[0]};

        # Ensure there's a joining token, otherwise use our default
        if( @qs > 1 ) {
            my $prev_query = 0;
            my @joined = ();
            foreach my $part ( @qs ) {
                if( $prev_query ) {
                    push @joined, $self->default_join() unless exists $JOINING{$part};
                }
                push @joined, $part;
                $prev_query = exists $JOINING{$part} ? 0 : 1;
            }
            @qs = @joined;
        }
    }
    # $query->add_aggregation();
    $query->add_bool($context => { query_string => { query => join(' ', @qs) } }) if @qs;

    return $query;
}

# Builder Routines for QS Objects
sub _build_plugins {
    my $self    = shift;
    my $globals = es_globals('plugins');
    my $finder = Module::Pluggable::Object->new(
        search_path => ['App::ElasticSearch::Utilities::QueryString',@{ $self->search_path }],
        except      => [qw(App::ElasticSearch::Utilities::QueryString::Plugin)],
        instantiate => 'new',
    );
    my @plugins;
    foreach my $p ( sort { $a->priority <=> $b->priority || $a->name cmp $b->name }
        $finder->plugins( options => defined $globals ? $globals : {} )
    ) {
        debug(sprintf "Loaded %s with priority:%d", $p->name, $p->priority);
        push @plugins, $p;
    }
    return \@plugins;
}

# Return true
1;
__END__

=head1 TOKENS

The token expansion plugins can return undefined, which is basically a noop on the token.
The plugin can return a hash reference, which marks that token as handled and no other plugins
receive that token.  The hash reference may contain:

=over 2

=item query_string

This is the rewritten bits that will be reassembled in to the final query string.

=item condition

This is usually a hash reference representing the condition going into the bool query. For instance:

    { terms => { field => [qw(alice bob charlie)] } }

Or

    { prefix => { user_agent => 'Go ' } }

These conditions will wind up in the B<must> or B<must_not> section of the B<bool> query depending on the
state of the the invert flag.

=item invert

This is used by the bareword "not" to track whether the token invoked a flip from the B<must> to the B<must_not>
state.  After each token is processed, if it didn't set this flag, the flag is reset.

=item dangles

This is used for bare words like "not", "or", and "and" to denote that these terms cannot dangle from the
beginning or end of the query_string.  This allows the final pass of the query_string builder to strip these
words to prevent syntax errors.

=back

=head1 Extended Syntax

The search string is pre-analyzed before being sent to ElasticSearch.  The following plugins
work to manipulate the query string and provide richer, more complete syntax for CLI applications.

=from_other App::ElasticSearch::Utilities::QueryString::BareWords / SYNOPSIS

=from_other App::ElasticSearch::Utilities::QueryString::IP / SYNOPSIS

=from_other App::ElasticSearch::Utilities::QueryString::Ranges / SYNOPSIS

=from_other App::ElasticSearch::Utilities::QueryString::Underscored / SYNOPSIS

=from_other App::ElasticSearch::Utilities::QueryString::FileExpansion / SYNOPSIS

