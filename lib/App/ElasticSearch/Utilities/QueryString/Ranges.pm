package App::ElasticSearch::Utilities::QueryString::Ranges;
# ABSTRACT: Implement parsing comparison operators to Equivalent Lucene syntax

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 20; }

my %Operators = (
    '<'  => { side => 'left',  op => 'lt' },
    '<=' => { side => 'left',  op => 'lte' },
    '>'  => { side => 'right', op => 'gt' },
    '>=' => { side => 'right', op => 'gte' },
);
my $op_match = join('|', map { quotemeta } sort { length $b <=> length $a } keys %Operators);

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my ($self,$token) = @_;

    debug(sprintf "%s - evaluating token '%s'", $self->name, $token);
    my ($k,$v) = split /:/, $token, 2;

    return unless $v;

    my %sides = ();
    my %range = ();
    foreach my $range (split /\,/, $v) {
        if( my($symbol,$value) = ( $range =~ /^($op_match)(.+)$/ ) ) {
            my $side = $Operators{$symbol}->{side};
            # Invalid query if two left or right operators
            die "attempted to set more than one $side-side operator in Range: $token"
                if $sides{$side};
            $sides{$side} = 1;
            $range{$Operators{$symbol}->{op}} = $value;
        }
    }

    return unless scalar keys %range;

    return { condition => { range => { $k => \%range } } };
}

# Return True;
1;
__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::Range

This plugin translates some special comparison operators so you don't need to
remember them anymore.

Example:

    price:<100

Will translate into a:

    { range: { price: { lt: 100 } } }

And:

    price:>50,<100

Will translate to:

    { range: { price: { gt: 50, lt: 100 } } }


=head3 Supported Operators

=over 2

B<gt> via E<gt>, B<gte> via E<gt>=, B<lt> via E<lt>, B<lte> via E<lt>=

=back
