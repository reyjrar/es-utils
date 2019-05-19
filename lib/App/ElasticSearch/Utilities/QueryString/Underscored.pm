package App::ElasticSearch::Utilities::QueryString::Underscored;
# ABSTRACT: Extend some _<type>_ queries

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 20; }

my %Underscored = (
    _prefix_ => sub {
        my ($v) = @_;
        my ($field,$text) = split /[:=]/, $v, 2;

        return unless defined $text and length $text;
        return { condition => { prefix => { $field => $text } } }
    },
);

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my ($self,$token) = @_;

    debug(sprintf "%s - evaluating token '%s'", $self->name, $token);
    my ($k,$v) = split /:/, $token, 2;

    return unless exists $Underscored{lc $k} and defined $v;

    return $Underscored{lc $k}->($v);
}

# Return True;
1;
__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::QueryString::Underscored

This plugin translates some special underscore surrounded tokens into
the Elasticsearch Query DSL.

Implemented:

=head3 _prefix_

Example query string:

    _prefix_:useragent:'Go '

Translates into:

    { prefix => { useragent => 'Go ' } }
