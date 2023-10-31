package App::ElasticSearch::Utilities::QueryString::Text;
# ABSTRACT: Provides a better interface for text and keyword queries

use v5.16;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use Const::Fast;
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 4; }

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my ($self,$token) = @_;

    my $meta = $self->fields_meta;

    debug(sprintf "%s - evaluating token '%s'", $self->name, $token);
    if ( $token =~ /[^:]+:/ ) {
        my ($f,$v) = split /:/, $token, 2;

        my $matcher = '';

        # Grab the prefix symbol
        $f =~ s/^(?<op>[^a-zA-Z])//;
        if( $+{op} ) {
            $matcher = $+{op} eq '*' ? 'wildcard'
                     : $+{op} eq '=' ? 'term'
                     : $+{op} eq '/' ? 'regexp'
                     : $+{op} eq '~' ? 'fuzzy'
                     : $+{op} eq '+' ? 'match_phrase'
                     : '';
        }

        # Check metadata for text type
        if ( exists $meta->{$f}
                && exists $meta->{$f}{type}
                && $meta->{$f}{type} eq 'text'
        ) {
            # We can't use term filters on text fields
            $matcher = 'match' if !$matcher or $matcher eq 'term';
        }

        if( $matcher ) {
            return { condition => { $matcher => { $f => $v } } };
        }
    }

    return;
}

# Return True;
1;

__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::QueryString::Text

Provides field prefixes to manipulate the text search capabilities.

=head3 Terms Query via '='

Provide an '=' prefix to a query string parameter to promote that parameter to a C<term> filter.

This allows for exact matches of a field without worrying about escaping Lucene special character filters.

E.g.:

    user_agent:"Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1"

Is evaluated into a weird query that doesn't do what you want.   However:

    =user_agent:"Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1"

Is translated into:

    { term => { user_agent => "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1" } }

=head3 Wildcard Query via '*'

Provide an '*' prefix to a query string parameter to promote that parameter to a C<wildcard> filter.

This uses the wild card match for text fields to making matching more intuitive.

E.g.:

    *user_agent:"Mozilla*"

Is translated into:

    { wildcard => { user_agent => "Mozilla* } }

=head3 Regexp Query via '/'

Provide an '/' prefix to a query string parameter to promote that parameter to a C<regexp> filter.

If you want to use regexp matching for finding data, you can use:

    /message:'\\bden(ial|ied|y)'

Is translated into:

    { regexp => { message => "\\bden(ial|ied|y)" } }

=head3 Fuzzy Matching via '~'

Provide an '~' prefix to a query string parameter to promote that parameter to a C<fuzzy> filter.

    ~message:deny

Is translated into:

    { fuzzy => { message => "deny" } }

=head3 Phrase Matching via '+'

Provide an '+' prefix to a query string parameter to promote that parameter to a C<match_phrase> filter.

    +message:"login denied"

Is translated into:

    { match_phrase => { message => "login denied" } }

=head3 Automatic Match Queries for Text Fields

If the field meta data is provided and the field is a C<text> type, the query
will automatically be mapped to a C<match> query.

    # message field is text
    message:"foo"

Is translated into:

    { match => { message => "foo" } }


=cut

