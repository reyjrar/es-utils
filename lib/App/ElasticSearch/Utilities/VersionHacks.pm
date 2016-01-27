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

my %SIMPLE = (
    '_nodes' => {
        0.17    => '_cluster/nodes',
        0.18    => '_cluster/nodes',
        0.19    => '_cluster/nodes',
        0.90    => '_cluster/nodes',
    },
    '_cluster/nodes' => {
        1.0     => '_nodes',
        1.1     => '_nodes',
        1.2     => '_nodes',
        1.3     => '_nodes',
        1.4     => '_nodes',
        1.5     => '_nodes',
        1.6     => '_nodes',
        1.7     => '_nodes',
        2.0     => '_nodes',
        2.1     => '_nodes',
    }
);
my %CALLBACKS = (
    '_cluster/state' => {
        0.17 => \&_cluster_state_legacy,
        0.18 => \&_cluster_state_legacy,
        0.19 => \&_cluster_state_legacy,
        0.90 => \&_cluster_state_legacy,
        1.0 => \&_cluster_state_1_0,
        1.1 => \&_cluster_state_1_0,
        1.2 => \&_cluster_state_1_0,
        1.3 => \&_cluster_state_1_0,
        1.4 => \&_cluster_state_1_0,
        1.5 => \&_cluster_state_1_0,
        1.6 => \&_cluster_state_1_0,
        1.7 => \&_cluster_state_1_0,
        2.0 => \&_cluster_state_1_0,
        2.1 => \&_cluster_state_1_0,
    },
);

my $version;

sub _fix_version_request {
    my ($url,$options,$data) = @_;

    # Requires App::ElasticSearch::Utilities to be loaded
    if( ! defined $version  ){
        eval {
            $version = App::ElasticSearch::Utilities::_get_es_version();
        };
        if(my $err = $@) {
            output({stderr=>1,color=>'red'}, "Failed version detection!", $@);
        }
        if ($version < 1.0) {
            output({stderr=>1,color=>'red',sticky=>1}, "!!! Detected ElasticSearch Version '$version', which is not going to be supported in future releases !!!");
        }
    }

    return @_ unless defined $version;

    if(exists $SIMPLE{$url}) {
        my $versions = join(", ", sort keys %{ $SIMPLE{$url} });
        debug("Method changed in API, evaluating rewrite ($versions) against $version");
        if(exists $SIMPLE{$url}->{$version}) {
            debug({indent=>1,color=>'yellow'}, "+ Rewriting $url to $SIMPLE{$url}->{$version}");
            $url = $SIMPLE{$url}->{$version};
        }
    }
    else {
        my $cb;
        foreach my $check (keys %CALLBACKS) {
            next unless $url =~ /^\Q$check\E/i;
            $cb = $check;
            last;
        }
        if( defined $cb ) {
            my $versions = join(", ", sort keys %{ $CALLBACKS{$cb} });
            debug("Method changed in API, evaluating callback for $cb ($versions) against $version");
            if(exists $CALLBACKS{$url}->{$version}) {
                debug({indent=>1,color=>'yellow'}, "+ Callback dispatched for $url");
                ($url,$options,$data) = $CALLBACKS{$url}->{$version}->($url,$options,$data);
            }
        }
    }

    return ($url,$options,$data);
}

my %_cluster_state = (
    nodes         => 0,
    routing_table => 0,
    metadata      => 0,
    indices       => 0,
    blocks        => 0,
    version       => 1.0,
    master_node   => 1.0,
);

sub _cluster_state_legacy {
    my ($url,$options,$data) = @_;

    # Handle the URL
    my (@parts) = split /\//, $url;
    $url = '_cluster/state';

    # Legacy doesn't support index names in the URI
    verbose({color=>"yellow"}, "Warning: _cluster/state doesn't support index names in ES $version")
        if @parts > 3;

    # Handle the requested metrics on a new fangled cluster
    if(@parts > 2 && length $parts[2]) {
        verbose({color=>'yellow'}, "WARNING: Attempting to use post 1.0 API for _cluster/state on ES $version");
        verbose({level=>2,indent=>1}, "See: http://www.elasticsearch.org/guide/en/reference/$version/cluster-state.html#cluster-state");
        my %requested = map { $_ => 1 } split /\,/, $parts[2];
        foreach my $metric (grep { $version >= $_cluster_state{$_} } keys %_cluster_state) {
            $options->{uri_param}->{"filter_$metric"} = exists $requested{$metric} ? 0 : 1;
        }
    }
}
sub _cluster_state_1_0 {
    my ($url,$options,$data) = @_;

    my @parts = split /\//, $url;

    # Translate old to new
    debug(sprintf("GOT %s with %d thingies", $url, scalar(@parts)));
    if( @parts < 3 ) {
        verbose({color=>'yellow'}, "DEPRECATION: Attempting to use legacy API for _cluster/state on ES $version");
        verbose({level=>2,indent=>1}, "See: http://www.elasticsearch.org/guide/en/reference/$version/cluster-state.html#cluster-state");
        my @requested = ();
        if( exists $options->{uri_param} ) {
            my %filters =
                    map { s/filter_//; $_ => 1; }
                    grep { /^filter_/ && $options->{uri_param}{$_} }
                keys %{ $options->{uri_param} };
            # Remove them from the parameters
            delete $options->{uri_param}{"filter_$_"} for keys %filters;
            if(keys %filters) {
                foreach my $metric (grep { $version >= $_cluster_state{$_} } keys %_cluster_state) {
                    push @requested, $metric unless exists $filters{$metric};
                }
            }
            else {
                push @requested, '_all';
            }
        }
        push @parts, join(',', @requested);
        my $new_url = join('/',@parts);
        verbose("~ Cluster State rewritten from $url to $new_url");
        $url=$new_url;
    }
    return ($url,$options,$data);
}

1;
