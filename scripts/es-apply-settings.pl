#!/usr/bin/env perl
# PODNAME: es-apply-settings.pl
# ABSTRACT: Run to apply a JSON list of settings to indexes matching a pattern
use strict;
use warnings;

use DateTime;
use JSON;
use Getopt::Long;
use Pod::Usage;
use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(:indices);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'dry-run',
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

my %CFG = (
    'dry-run' => 0,
);
# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    $CFG{$setting} = $opt{$setting} if exists $opt{$setting} and defined $opt{$setting};
}
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

# Grab an ElasticSearch connection
my $es = es_connect();

# Delete Indexes older than a certain point
my $d_res = $es->cluster_state(
    filter_nodes         => 1,
    filter_routing_table => 1,
);
my $indices = $d_res->{metadata}{indices};
if ( !defined $indices ) {
    output({color=>"red"}, "Unable to locate indices in status!");
    exit 1;
}
# Loop through the indices and take appropriate actions;
foreach my $index (sort keys %{ $indices }) {
    verbose("$index being evaluated");

    next unless $index =~ /^$opt{pattern}/;
    verbose({color=>'yellow'}, " + matched pattern, checking settings");

    my $current = undef;
    eval {
        $current = $es->index_settings( index => $index );
    };
    if( my $err = $@ ) {
        output({color=>'magenta'}, " + Unable to fetch index settings, applying blind!");
    }

    if( ! $CFG{'dry-run'} ) {
        my $result = undef;
        # Close the index first
        if (exists $opt{close} && $opt{close}) {
            eval {
                $result = $es->close_index(index => $index);
            };
            if ( my $err = $@ ) {
                output({color=>"red"}, "Closing index $index failed.", $err);
                next;
            }
            output({color=>'cyan'}, " + Closed $index to apply settings.");
        }

        eval {
            $result = $es->update_index_settings(
                index    => $index,
                settings => $settings,
            );
        };
        if( my $err = $@ ) {
            output({color=>'red'}, "Unable to update settings on $index", $err);
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
            eval {
                $result = $es->open_index(index => $index);
            };
            if ( my $err = $@ ) {
                output({color=>"red"}, " + Opening index $index failed.", $err);
                next;
            }
            output({color=>'cyan'}, " + Re-opening $index with new settings.");
        }
    }
    else {
        output({color=>"cyan"}, " + Would have applied settings.");
    }
}

__END__

=head1 SYNOPSIS

es-apply-settings.pl --local --pattern logstash-* settings.json

Options:

    --help              print help
    --manual            print full manual
    --dry-run           Don't apply settings, just tell me what you would do
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

=item B<dry-run>

Only tell me what you would do, don't actually perform any action

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
