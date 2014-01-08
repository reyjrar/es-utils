#!/usr/bin/env perl
# PODNAME: es-apply-settings.pl
# ABSTRACT: Run to apply a JSON list of settings to indexes matching a pattern
use strict;
use warnings;

BEGIN {
    # Clear out any proxy settings
    delete $ENV{$_} for qw(http_proxy HTTP_PROXY);
}

use DateTime;
use Elasticsearch::Compat;
use JSON;
use LWP::Simple;
use Getopt::Long qw(:config posix_default no_ignore_case no_ignore_case_always);
use Pod::Usage;
use App::ElasticSearch::Utilities qw(:all);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'dry-run',
    'local',
    'host:s',
    'port:i',
    'pattern:s',
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
# Host or Local
pod2usage(1) unless defined $opt{local} or defined $opt{host};

my %CFG = (
    port      => 9200,
    'dry-run' => 0,
);
# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    $CFG{$setting} = $opt{$setting} if exists $opt{$setting} and defined $opt{$setting};
}
if ( ! exists $opt{pattern} ) {
    pod2usage(1);
}
my %REGEX = (
    '*'  => qr/.*/,
    '?'  => qr/.?/,
    DATE => qr/\d{4}[.\-]?\d{2}[.\-]?\d{2}/,
    ANY  => qr/.*/,
);
foreach my $literal ( keys %REGEX ) {
    $opt{pattern} =~ s/\Q$literal\E/$REGEX{$literal}/g;
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

# Create the target uri for the ES Cluster
my $TARGET = exists $opt{host} && defined $opt{host} ? $opt{host} : 'localhost';
$TARGET .= ":$CFG{port}";
debug("Target is: $TARGET");
debug_var(\%CFG);

my $es = Elasticsearch::Compat->new(
    servers   => [ $TARGET ],
    transport => 'http',
    timeout   => 0,     # Do Not Timeout
);

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
    --local             Poll localhost and use name reported by ES
    --host|-H           Host to poll for statistics
    --dry-run           Don't apply settings, just tell me what you would do
    --local             Assume localhost as the host
    --pattern           Apply to indexes whose name matches this pattern
    --close             Close the index, apply settings, and re-open the index
    --quiet             Ideal for running on cron, only outputs errors
    --verbose           Send additional messages to STDERR

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<manual>

Print this message and exit

=item B<local>

Optional, operate on localhost (if not specified, --host required)

=item B<host>

Optional, the host to maintain (if not specified --local required)

=item B<pattern>

B<REQUIRED>: Use this pattern to match indexes

=item B<close>

B<IMPORTANT>: Settings are not dynamic, and the index needs to closed to have
the settings applied.  If this is set, the index will be re-opened before moving to the
next index.

=item B<dry-run>

Only tell me what you would do, don't actually perform any action

=item B<verbose>

Verbose stats, to not interfere with cacti, output goes to STDERR

=back

=head1 DESCRIPTION

This script allows you to change index settings on indexes whose name matches the specified pattern.

Usage:

    $ es-apply-settings.pl --local --pattern logstash-*
    > { "index.routing.allocation.exclude.ip": "192.168.10.120" }

Or specify a file containing the settings

    $ es-apply-settings.pl --local --pattern logstash-* settings.json

=head2 PATTERNS

Patterns are used to match an index to the aliases it should have.  A few symbols are expanded into
regular expressions.  Those patterns are:

    *       expands to match any number of any characters.
    ?       expands to match any single character.
    DATE    expands to match YYYY.MM.DD, YYYY-MM-DD, or YYYYMMDD
    ANY     expands to match any number of any characters.

=cut
