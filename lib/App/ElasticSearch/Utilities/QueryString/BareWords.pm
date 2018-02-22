package App::ElasticSearch::Utilities::QueryString::BareWords;
# ABSTRACT: Mostly fixing case and tracking dangling words

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 1; }

my %BareWords = (
    and => { query_string => 'AND', invert => 0, dangles => 1 },
    or  => { query_string => 'OR',  invert => 0, dangles => 1 },
    not => { query_string => 'NOT', invert => 1, dangles => 1 },
);

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my ($self,$token) = @_;

    debug(sprintf "%s - evaluating token '%s'", $self->name, $token);
    return exists $BareWords{lc $token} ? [$BareWords{lc $token}] : undef;
}

# Return True;
1;

__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::Barewords

The following barewords are transformed:

    or => OR
    and => AND
    not => NOT
