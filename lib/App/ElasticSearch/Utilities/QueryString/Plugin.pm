package App::ElasticSearch::Utilities::QueryString::Plugin;
# ABSTRACT: Moo::Role for implementing QueryString Plugins

use Hash::Merge::Simple qw(clone_merge);
use Moo::Role;
use Sub::Quote;


=head1 INTERFACE

=head2 handle_token()

The handle_token() routine receives a single token from the command line, often a single word
and returns a hash reference specifying

=from_other App::ElasticSearch::Utilities::QueryString / TOKENS / all

=cut

requires qw(handle_token);

# Attributes
has name => (
    is => 'ro',
    isa => quote_sub(q{die "Needs to be a string" if ref $_[0]}),
    builder => '_build_name',
    lazy => 1,
);
has priority => (
    is      => 'ro',
    isa     => quote_sub(q{die "Not between 1 and 100" unless $_[0] > 0 && $_[0] <= 100 }),
    builder => '_build_priority',
    lazy    => 1,
);

around 'handle_token' => sub {
    my $orig = shift;
    my $self = shift;
    my $refs = $orig->($self,@_);
    if( defined $refs ) {
        if( ref $refs eq 'ARRAY' ) {
            foreach my $doc (@{ $refs }) {
                $doc->{_by} = $self->name;
            }
        }
        elsif( ref $refs eq 'HASH' ) {
            $refs->{_by} = $self->name;
        }
        return $refs;
    }
    else {
        return;
    }
};

# Builders
sub _build_name {
    my $self = shift;
    my $class = ref $self;
    return (split /::/, $class)[-1];
}
sub _build_priority { 50; }

# Handle Build Args
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
