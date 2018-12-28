package App::ElasticSearch::Utilities::QueryString::AutoEscape;
# ABSTRACT: Automatically escape characters that have special meaning in
# Lucene

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 1; }

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my ($self,$token) = @_;

    debug(sprintf "%s - evaluating token '%s'", $self->name, $token);
    return [{ query_string => $token =~ s{([/ ()])}{\\$1}gr}];
}

# Return True;
1;

__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::AutoEscape

Escapes characters in the query string that have special meaning in Lucene.

Characters escaped are: ' ', '/', '(', and ')'
