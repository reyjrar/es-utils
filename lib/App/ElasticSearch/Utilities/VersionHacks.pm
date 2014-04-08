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

sub _fix_version_request {
    my ($url,$options,$data) = @_;

    my $version = $App::ElasticSearch::Utilities::CURRENT_VERSION;

    return @_ unless defined $version;

    if(exists $URI{$url}) {
        my $versions = join(", ", sort keys %{ $URI{$url} });
        debug("Method changed in API, evaluating rewrite ($versions) against $version");
        if(exists $URI{$url}->{$version}) {
            debug({indent=>1,color=>'yellow'}, "+ Rewriting $url to $URI{$url}->{$version}");
            $url = $URI{$url}->{$version};
        }
    }

    return ($url,$options,$data);
}

1;
