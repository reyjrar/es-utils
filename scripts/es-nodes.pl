#!perl
# PODNAME: es-nodes.pl
# ABSTRACT: Listing the nodes in a cluster with some details
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_request);
use CLI::Helpers qw(:output);
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT,
    'attributes|attr:s',
    'help|h',
    'manual|m',
);


#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitval => 0, -verbose => 2) if $OPT{manual};

my $cres = es_request('_cluster/health');
my $CLUSTER = defined $cres ? $cres->{cluster_name} : 'UNKNOWN';

output({clear=>1,color=>'magenta'}, "Cluster [$CLUSTER] contains $cres->{number_of_nodes} nodes.", '-='x20);
# Get a list of nodes
my $nres = es_request('_cluster/state/master_node,nodes', {});
if(!defined $nres) {
    output({stderr=>1,color=>'red'}, 'Fetching node status failed.');
    exit 1;
}
debug_var($nres);
foreach my $uuid (sort { $nres->{nodes}{$a}->{name} cmp $nres->{nodes}{$b}->{name} } keys %{ $nres->{nodes} }) {
    my $node = $nres->{nodes}{$uuid};
    my $color = defined $nres->{master_node} && $uuid eq $nres->{master_node} ? 'green' : 'cyan';

    output({color=>$color}, $node->{name});
    output({indent=>1,kv=>1,color=>$color}, address => $node->{transport_address});
    verbose({indent=>1,kv=>1,color=>$color}, uuid => $uuid);
    if( exists $OPT{attributes} ) {
        output({indent=>1}, "attributes:");
        foreach my $attr ( split /,/, $OPT{attributes} ) {
            next unless exists $node->{attributes}{$attr};
            output({indent=>2,kv=>1}, $attr => $node->{attributes}{$attr});
        }
    }
}

__END__

=head1 NAME

es-nodes.pl - Utility for investigating the nodes in a cluster

=head1 SYNOPSIS

es-nodes.pl [options]

Options:

    --help              print help
    --manual            print full manual
    --attibutes         Comma separated list of attributes to display, default is NONE

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=item B<attributes>

Comma separated list of node attributes to display, aliased as --attr

    --attributes dc,id

=back

=head1 DESCRIPTION

This tool provides access to information on nodes in the the cluster.
