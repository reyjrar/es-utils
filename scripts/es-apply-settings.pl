#!/usr/bin/env perl
# PODNAME: es-apply-settings.pl
# ABSTRACT: Run to apply a JSON list of settings to indexes matching a pattern
use strict;
use warnings;

use JSON::XS;
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use Pod::Usage;
use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(:default es_apply_index_settings es_open_index es_close_index);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'close',
    # Basic options
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

#------------------------------------------------------------------------#

# Read JSON Settings
my $RawJSON = '';
$RawJSON .= $_ while <>;

my $settings = undef;
eval {
    $settings = decode_json $RawJSON;
};
if( my $err = $@ ) {
    output({color=>'red'}, "Invalid JSON structure passed, error was '$err'");
    debug('JSON Passed was:', $RawJSON);
    exit 1;
}
debug("Settings to apply");
debug_var($settings);

# Delete Indexes older than a certain point
my @indices = es_indices();
if ( !@indices ) {
    output({color=>"red"}, "No matching indices found.");
    exit 1;
}
# Loop through the indices and take appropriate actions;
foreach my $index (sort @indices) {
    verbose("$index:  evaluated");

    my $current = es_request('_settings', {index=>$index});
    if( !defined $current ) {
        output({color=>'magenta'}, " + Unable to fetch index settings, applying blind!");
    }

    # Close the index first
    if (exists $opt{close} && $opt{close}) {
        my $res = es_close_index($index);
        if ( !defined $res ) {
            output({color=>"red"}, "Closing index $index failed.");
            next;
        }
        output({color=>'cyan'}, " + Closed $index to apply settings.");
    }

    my $result = es_apply_index_settings($index,$settings);
    if( !defined $result ) {
        output({color=>'red'}, "Unable to update settings on $index");
        debug("Current");
        debug_var($current);
    }
    else {
        output({color=>'green'}, " + Settings applied successfully!");
    }
    debug({color=>"cyan"},"Result was:");
    debug_var($result);

    # Re-open the index
    if (exists $opt{close} && $opt{close}) {
        my $result = es_open_index($index);
        if ( !defined($result) ) {
            output({color=>"red"}, " + Opening index $index failed.");
            next;
        }
        output({color=>'cyan'}, " + Re-opening $index with new settings.");
    }
}

__END__

=head1 SYNOPSIS

es-apply-settings.pl --local --pattern logstash-* settings.json

Options:

    --help              print help
    --manual            print full manual
    --close             Close the index, apply settings, and re-open the index

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<close>

B<IMPORTANT>: Settings are not dynamic, and the index needs to closed to have
the settings applied.  If this is set, the index will be re-opened before moving to the
next index.

=back

=head1 DESCRIPTION

This script allows you to change index settings on indexes whose name matches the specified pattern.

Usage:

    $ es-apply-settings.pl --local --pattern logstash-*
    > { "index.routing.allocation.exclude.ip": "192.168.10.120" }

Or specify a file containing the settings

    $ es-apply-settings.pl --local --pattern logstash-* settings.json

=from_other App::ElasticSearch::Utilities / PATTERNS / all

=cut
