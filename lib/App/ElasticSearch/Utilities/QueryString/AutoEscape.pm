package App::ElasticSearch::Utilities::QueryString::AutoEscape;
# ABSTRACT: Automatically escape characters that have special meaning in
# Lucene

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use Const::Fast;
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

const my $special_character_class => qr{[/() ]};

sub _build_priority { 75; }

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my ($self,$token) = @_;

    debug(sprintf "%s - evaluating token '%s'", $self->name, $token);
    my $escaped = $token =~ s/($special_character_class)/\\$1/gr;

    # No escaped characters, skip it
    return if $escaped eq $token;

    # Modify the token
    return { query_string => $escaped };
}

# Return True;
1;

__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::AutoEscape

Escapes characters in the query string that have special meaning in Lucene.

Characters escaped are: ' ', '/', '(', and ')'
