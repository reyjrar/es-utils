# ABSTRACT: Fix version issues to support all the things
package App::ElasticSearch::Utilities::VersionHacks;

# VERSION
use strict;
use warnings;

use CLI::Helpers qw(:all);
use Sub::Exporter -setup => {
    exports => [ qw(
        _fix_version_request
    )],
};

my %URI = (
    '_nodes' => {
        0.17    => '_cluster/nodes',
        0.18    => '_cluster/nodes',
        0.19    => '_cluster/nodes',
        0.90    => '_cluster/nodes',
    },
    '_cluster/nodes' => {
        1.0     => '_nodes',
        1.1     => '_nodes',
    }
);

our $CURRENT_VERSION;

sub _fix_version_request {
    my ($url,$options,$data) = @_;

    return @_ unless defined $CURRENT_VERSION;

    if(exists $URI{$url}) {
        my $version = join(", ", sort keys %{ $URI{$url} });
        debug("Method changed in API, evaluating rewrite ($version) against $CURRENT_VERSION");
        if(exists $URI{$url}->{$CURRENT_VERSION}) {
            debug({indent=>1,color=>'yellow'}, "+ Rewriting $url to $URI{$url}->{$CURRENT_VERSION}");
            $url = $URI{$url}->{$CURRENT_VERSION};
        }
    }

    return ($url,$options,$data);
}

1;
