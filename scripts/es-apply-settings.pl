#!perl
# PODNAME: es-apply-settings.pl
# ABSTRACT: Run to apply a JSON list of settings to indexes matching a pattern
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CLI::Helpers qw(:all);
use JSON::MaybeXS;
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use Pod::Usage;
use Test::Deep qw( cmp_details deep_diag subhashof );

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'skip-alias=s@',
    'close',
    'older',
    'no-skip|noskip',
    'no-diff|nodiff',
    # Basic options
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

#------------------------------------------------------------------------#

# Figure out the skipped aliases
my %SKIP = map { $_ => 1 } (
    qw( .hold .do_not_erase ),
    $opt{'skip-alias'} ? ( @{ $opt{'skip-alias'} } ) : (),
);

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
my @indices = es_indices(older => $opt{older});
if ( !@indices ) {
    output({color=>"red"}, "No matching indices found.");
    exit 1;
}
# Loop through the indices and take appropriate actions;
foreach my $index (sort @indices) {
    verbose("$index:  evaluated");

    my $info = es_request($index);
    next unless $info->{$index};
    my $meta = $info->{$index};

    # Only safety check when not forced
    if( !$opt{'no-skip'} ) {
        my $skipped;
        foreach my $alias ( keys %{ $meta->{aliases} } ) {
            $skipped = $alias if exists $SKIP{$alias};
            last if defined $skipped;
        }
        if( defined $skipped ) {
            output({color=>'magenta'}, " ~ Skipped $index due to protected alias '$skipped', use --no-skip to skip this safety check");
            next;
        }
    }

    # Check for settings first
    if( !$opt{'no-diff'} ) {
        # Since both, either, or neither can be flattened, flatten
        my $current = es_flatten_hash( $meta->{settings} );
        my $desired = es_flatten_hash( $settings );
        my ($ok,$stack) = cmp_details($desired, subhashof($current));
        if( $ok ) {
            output({color=>'cyan'}, " ~ Skipped $index as it already contains those settings, use --no-diff to apply without this check");
            next;
        }
        else {
            debug(deep_diag($stack));
        }
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
        debug_var($meta->{settings});
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

=item B<older>

When this option is used along with the --days option the the setting will only be applied
to indexs that are older than the days specified.

    es-apply-settings.pl --older --days 30 --pattern logstash-*

=item B<skip-alias>

Protected aliases, which if present will cause the application of settings to
be skipped for a particular index.  The aliases C<.hold> and C<.do_not_erase>
will always be skipped.

=item B<no-skip>

Apply settings to all matching indexes, regardless of the protected aliases.

=item B<no-diff>

During a normal run, the settings you're requesting will be checked against the
indices and only indices with settings out of line with the desired settings
will be applied.  If for some reason you want to apply settings regardless of
the current state, using C<--no-diff> will disable the check and apply the
settings to every index in scope.

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
