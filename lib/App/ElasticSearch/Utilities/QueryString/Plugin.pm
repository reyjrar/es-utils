package App::ElasticSearch::Utilities::QueryString::Plugin;
# ABSTRACT: Moo::Role for implementing QueryString Plugins

use v5.16;
use warnings;

# VERSION

use Hash::Merge::Simple qw(clone_merge);
use Moo::Role;
use Ref::Util qw(is_arrayref is_hashref);
use Types::Standard qw( Str Int );

=attr name

Name of the plugin, used in debug reporting.

=cut

# Attributes
has name => (
    is  => 'lazy',
    isa => Str,
);
sub _build_name {
    my $self = shift;
    my $class = ref $self;
    return (split /::/, $class)[-1];
}

=attr priority

Priority is an integer which determmines the order tokens are parsed in
low->high order.

=cut

has priority => (
    is  => 'lazy',
    isa => Int,
);
sub _build_priority { 50; }

=head1 INTERFACE

=head2 handle_token()

The handle_token() routine receives a single token from the command line, often a single word
and returns a hash reference specifying

=from_other App::ElasticSearch::Utilities::QueryString / TOKENS / all

=cut

requires qw(handle_token);

around 'handle_token' => sub {
    my $orig = shift;
    my $self = shift;
    my $refs = $orig->($self,@_);
    if( defined $refs ) {
        if( is_arrayref($refs) ) {
            foreach my $doc (@{ $refs }) {
                $doc->{_by} = $self->name;
            }
        }
        elsif( is_hashref($refs) ) {
            $refs->{_by} = $self->name;
        }
        return $refs;
    }
    else {
        return;
    }
};


# Handle Build Args

=for Pod::Coverage BUILDARGS

=cut

sub BUILDARGS {
    my($class,%in) = @_;

    my @search = map { $_ => lc $_ } ($class,(split /::/, $class)[-1]);
    my $options = exists $in{options} ? delete $in{options} : {};

    my @options = ();
    foreach my $s (@search) {
        if( exists $options->{$s} ) {
            push @options, $options->{$s};
            last;
        }
    }
    push @options, \%in if keys %in;

    return scalar(@options) ? clone_merge(@options) : {};
}

1;
