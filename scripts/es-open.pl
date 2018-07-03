#!perl
# PODNAME: es-open.pl
# ABSTRACT: Open any closed indices matching your paramters.
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_indices es_request);
use CLI::Helpers qw(:output);
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT,
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitval => 0, -verbose => 2) if $OPT{manual};

# Return all closed indexes within our constraints.
my @indices = es_indices(state => 'closed');

foreach my $idx (reverse sort @indices) {
    verbose("Opening index: $idx");
    my $result = es_request('_open', { index=>$idx, method => 'POST'});
    debug_var($result);
    my $color = 'green';
    output({color=>$color}, "+ Opened '$idx'");
}

__END__

=head1 NAME

es-open.pl - Utility for opening indices that are closed mathcing the constraints.

=head1 SYNOPSIS

es-open.pl [options]

Options:

    --help              print help
    --manual            print full manual

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=back

=head1 DESCRIPTION

This tool provides access to open any closed indices in the cluster
matching the parameters.

Open the last 45 days of logstash indices:

    es-open.pl --base logstash --days 45

