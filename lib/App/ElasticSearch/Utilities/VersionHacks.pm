# ABSTRACT: Fix version issues to support all the things
package App::ElasticSearch::Utilities::VersionHacks;

# VERSION
use strict;
use warnings;

use Const::Fast;
use CLI::Helpers qw(:all);
use Sub::Exporter -setup => {
    exports => [ qw(
        _fix_version_request
    )],
};

const my $MIN_VERSION => 1.0;
const my %SIMPLE => (
    '_cluster/nodes' => {
        default => '_nodes',
    },
    '_optimize' => {
        # Yes, in case you're wondering _optimize disappeared in 2.2 after being deprecated in 2.1
        default => '_forcemerge',
        'v1.0'  => '_optimize',
        'v1.1'  => '_optimize',
        'v1.2'  => '_optimize',
        'v1.3'  => '_optimize',
        'v1.4'  => '_optimize',
        'v1.5'  => '_optimize',
        'v1.6'  => '_optimize',
        'v1.7'  => '_optimize',
        'v1.8'  => '_optimize',
        'v1.9'  => '_optimize',
        'v2.0'  => '_optimize',
    },
    '_status' => {
        default => '_stats',
        'v1.0'  => '_status',
        'v1.1'  => '_status',
    }
);
my %CALLBACKS = (
    '_cluster/state' => {
        default => \&_cluster_state_1_0,
    },
);

my $version;

sub _fix_version_request {
    my ($url,$options,$data) = @_;

    # Requires App::ElasticSearch::Utilities to be loaded
    if( ! defined $version  ){
        eval {
            $version = sprintf "%0.1f", App::ElasticSearch::Utilities::_get_es_version();
            1;
        } or do {
            my $err = $@;
            output({stderr=>1,color=>'red'}, "Failed version detection!", $@);
        };
        if (defined $version && $version < $MIN_VERSION) {
            output({stderr=>1,color=>'red',sticky=>1},
                    "!!! Detected ElasticSearch Version '$version', which is < $MIN_VERSION, please upgrade your cluster !!!");
            exit 1;
        }
    }

    my $vstr = sprintf "v%0.1f", $version;

    if(exists $SIMPLE{$url}) {
        my $versions = join(", ", sort keys %{ $SIMPLE{$url} });
        debug("Method changed in API, evaluating rewrite ($versions) against $vstr");
        if(exists $SIMPLE{$url}->{$vstr}) {
            debug({indent=>1,color=>'yellow'}, "+ Rewriting $url to $SIMPLE{$url}->{$vstr}");
            $url = $SIMPLE{$url}->{$vstr};
        }
        elsif(exists $SIMPLE{$url}->{default}) {
            debug({indent=>1,color=>'yellow'}, "+ Rewriting $url to $SIMPLE{$url}->{default} by default rule");
            $url = $SIMPLE{$url}->{default};
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
            debug("Method changed in API, evaluating callback for $cb ($versions) against $vstr");
            if(exists $CALLBACKS{$url}->{$vstr}) {
                debug({indent=>1,color=>'yellow'}, "+ Callback dispatched for $url");
                ($url,$options,$data) = $CALLBACKS{$url}->{$vstr}->($url,$options,$data);
            }
            elsif(exists $CALLBACKS{$url}->{default}) {
                debug({indent=>1,color=>'yellow'}, "+ Callback dispatched for $url by default rule");
                ($url,$options,$data) = $CALLBACKS{$url}->{default}->($url,$options,$data);
            }
        }
    }

    return ($url,$options,$data);
}

my %_cluster_state = map { $_ => 1  } qw(
    nodes
    routing_table
    metadata
    indices
    blocks
    version
    master_node
);

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
                foreach my $metric (keys %_cluster_state) {
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
