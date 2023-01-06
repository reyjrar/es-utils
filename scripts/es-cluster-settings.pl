#!perl
# PODNAME: es-cluster-settings.pl
# ABSTRACT: Get or apply settings to the cluster
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CLI::Helpers qw(:all);
use Const::Fast;
use JSON::MaybeXS;
use Getopt::Long::Descriptive;
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
const my %DEFAULT => (
    duration => 'transient',
);
my ($opt,$usage) = describe_options('%c %o',
    ['duration=s', hidden =>
        {
            default => $DEFAULT{duration},
            one_of => [
                [ 'transient|t' => "Apply to the transient settings, the default", { implies => { duration => 'transient' } } ],
                [ 'persistent|p' => "Apply to the persistent settings", { implies => { duration => 'persistent' } } ],
            ],
        },
    ],
    ['update|u=s%', "Settings in flat form to set, can be specified more than once, ie: -s search.max_buckets=1000000"],
    ['delete=s@',   "Settings in flat form to delete, can be specified more than once"],
    [],
    ['help', 'Display this message', { shortcircuit => 1 }],
    ['manual', 'Display full manual', { shortcircuit => 1 }],
);

#------------------------------------------------------------------------#
# Documentations!
if( $opt->help ) {
    print $usage->text;
    exit 0;
}
pod2usage(-exitstatus => 0, -verbose => 2) if $opt->manual;

#------------------------------------------------------------------------#
my $json = JSON->new->pretty->utf8->canonical;

my $current = es_request('/_cluster/settings', { uri_param => { flat_settings => 'true' } });
output({color=>'cyan'}, "-- Current Settings --");
output($json->encode($current));

if( $opt->update || $opt->delete ) {
    output({color=>'magenta',clear =>1}, sprintf "-- Updating Settings [%s] --", $opt->duration);

    # Add updates
    my %settings = $opt->update ? %{ $opt->update } : ();

    # Add deletes
    if( my $deletes = $opt->delete ) {
        foreach my $setting ( @{ $deletes } ) {
            $settings{$setting} = undef;
        }
    }

    # Peform the operation
    my $data = es_request('/_cluster/settings',
        {
            method => 'PUT',
            uri_param => { flat_settings => 'true' },
        },
        {
            $opt->duration => \%settings,
        }
    );

    die "Failed updating settings" unless $data;

    # Report success/failure
    if( my $ack = delete $data->{acknowledged} ) {
            output({color=>'green'}, "Successfully applied settings!");
    }
    else {
        output({color=>'red'}, "FAILED applying settings:");
    }
    output($json->encode($data));

    # Show resulting settings
    my $now = es_request('/_cluster/settings', { uri_param => { flat_settings => 'true' } });
    output({color=>'cyan'}, "-- Final Settings --");
    output($json->encode($now));
}



__END__

=head1 SYNOPSIS

es-cluster-settings.pl --update cluster.routing.allocation.exclude._name=node101

Options:

    --transient         Update the transient cluster settings, the default
    --persistent        Update the persistent cluster settings

    --update            Expects K=V in the flat form to update the cluster settings,
                        can be specified more than once:
                            --update search.max_buckets=10000000 \
                            --update cluster.routing.allocation.awareness.attributes=rack

    --delete            Name of a setting in flat form to delete, can be specified
                        more than once

                            --delete search.max_buckets --delete cluster.routing.allocation.awareness.*

    --help              print help
    --manual            print full manual

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 DESCRIPTION

This script allows you to change cluster settings easily.

Usage:

    # Show current settings
    $ es-cluster-settings.pl

    # Remove a node from shard allocation via the transient settings
    $ es-cluster-settings.pl --update cluster.routing.allocation.exclude._name=node-101

    # Update the search.max_buckets persistently
    $ es-cluster-settings.pl --persistent --update search.max_buckets=10000000

    # Delete the search.max_buckets from the transient settings
    $ es-cluster-settings.pl --delete search.max_buckets

    # Delete the cluster.routing.allocation.enabled in the persistent settings
    $ es-cluster-settings.pl --persistent --delete cluster.routing.allocation.enabled

    # Delete all the cluster.routing.allocation.* settings in the persistent section
    $ es-cluster-settings.pl --persistent --delete cluster.routing.allocation.*

=cut
