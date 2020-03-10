#!perl
# PODNAME: es-index-blocks.pl
# ABSTRACT: Report and manage index blocks
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(es_request es_node_stats es_index_stats es_index_strip_date es_flatten_hash);
use CLI::Helpers qw(:all);
use Getopt::Long::Descriptive;
use IO::Socket::INET;
use Pod::Usage;
use Ref::Util qw(is_hashref is_arrayref);

#------------------------------------------------------------------------#
# Argument Collection
my ($opt,$usage) = describe_options('%c %o',
    ['remove-blocks|remove',  "Remove discovered blocks, default is to just report."],
    [],
    ['For complete options, see the manual.'],
    [],
    ['help',   "Display this message and exit.", { shortcircuit => 1 }],
    ['manual', "Display complete man page.", { shortcircuit => 1 }],
);
#------------------------------------------------------------------------#
# Documentations!
if( $opt->help ) {
    print $usage->text;
    exit;
}
if( $opt->manual ) {
    pod2usage(-exitstatus => 0, -verbose => 2);
}

#------------------------------------------------------------------------#
# Get Index Blocks
my $result = es_request('_settings/index.blocks.*', { index => '_all' });
my %blocks=();
my @all_indices=();
foreach my $idx ( keys %{ $result } ) {
    push @all_indices, $idx;
    if( $result->{$idx}{settings} ) {
        my $settings = es_flatten_hash( $result->{$idx}{settings} );
        foreach my $block ( keys %{ $settings } ) {
            my $value = $settings->{$block};
            if( lc $value eq 'true') {
                push @{ $blocks{$block} }, $idx;
            }
        }
    }
}

#------------------------------------------------------------------------#
# Report Blocks
if( my @blocks = keys %blocks ) {
    foreach my $block ( sort @blocks ) {
        output({color=>'cyan',clear=>1}, "Index block: $block");
        foreach my $index (sort @{ $blocks{$block} }) {
            output({data=>1}, "$index is $block");
            if( $opt->remove_blocks ) {
                eval {
                    my $result = es_request('_settings',
                        { index  => $index, method => 'PUT' },
                        { $block => 'false' },
                    );
                    die "not acknowledged" unless $result->{acknowledged};
                    output({color=>'green',indent=>1}, "$block removed.");
                    1;
                } or do {
                    my $err = $@;
                    output({color=>'red',indent=>1}, "ERROR removing $block from $index: $err");
                };
            }
        }
    }
}
else {
    output({color=>'green'}, "No blocks discovered on any indices.");
    exit 0;
}

__END__

=head1 SYNOPSIS

es-index-blocks.pl --host [host] [options]

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

Print this message and exit

=back

=head1 DESCRIPTION

This script reports and optionally clears indexes with read_only_allow_delete set.

=cut
