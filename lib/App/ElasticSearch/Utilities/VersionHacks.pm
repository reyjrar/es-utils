# ABSTRACT: Fix version issues to support all the things
package App::ElasticSearch::Utilities::VersionHacks;

# VERSION
use strict;
use warnings;

use Const::Fast;
use CLI::Helpers qw(:all);
use Ref::Util qw(is_ref is_hashref);
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
        1.0 => '_optimize',
        1.1 => '_optimize',
        1.2 => '_optimize',
        1.3 => '_optimize',
        1.4 => '_optimize',
        1.5 => '_optimize',
        1.6 => '_optimize',
        1.7 => '_optimize',
        1.8 => '_optimize',
        1.9 => '_optimize',
        2.0 => '_optimize',
    },
    '_status' => {
        default => '_status',
        2.0     => '_stats',
        2.1     => '_stats',
        2.2     => '_stats',
        2.3     => '_stats',
        5.0     => '_stats',
    }
);
my %CALLBACKS = (
    '_cluster/state' => {
        default => \&_cluster_state_1_0,
    },
    '_search/scroll' => {
        default => \&_search_scroll_2_0,
        1.0     => \&_search_scroll_1_0,
        1.1     => \&_search_scroll_1_0,
        1.2     => \&_search_scroll_1_0,
        1.3     => \&_search_scroll_1_0,
        1.4     => \&_search_scroll_1_0,
        1.5     => \&_search_scroll_1_0,
        1.6     => \&_search_scroll_1_0,
        1.7     => \&_search_scroll_1_0,
        1.8     => \&_search_scroll_1_0,
        1.9     => \&_search_scroll_1_0,
    },
);

my $version;

sub _fix_version_request {
    my ($url,$options,$data) = @_;

    # Requires App::ElasticSearch::Utilities to be loaded
    if( ! defined $version  ){
        eval {
            $version = App::ElasticSearch::Utilities::_get_es_version();
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

    return @_ unless defined $version;

    if(exists $SIMPLE{$url}) {
        my $versions = join(", ", sort keys %{ $SIMPLE{$url} });
        debug("Method changed in API, evaluating rewrite ($versions) against $version");
        if(exists $SIMPLE{$url}->{$version}) {
            debug({indent=>1,color=>'yellow'}, "+ Rewriting $url to $SIMPLE{$url}->{$version}");
            $url = $SIMPLE{$url}->{$version};
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
            debug("Method changed in API, evaluating callback for $cb ($versions) against $version");
            if(exists $CALLBACKS{$url}->{$version}) {
                debug({indent=>1,color=>'yellow'}, "+ Callback dispatched for $url");
                ($url,$options,$data) = $CALLBACKS{$url}->{$version}->($url,$options,$data);
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

sub search_scroll_2_0 {
    my ($url,$options,$data) = @_;

    # Translate 1.0 version of scrolls to the 2.0 version
    my %params = ();
    if( defined $data ) {
        if( is_hashref($data) ) {
            %params = %{ $data };
        }
        elsif( length $data ) {
            $params{scroll_id} = $data;
            if( exists $options->{uri_param} ) {
                $params{scroll} = exists $options->{uri_param}{scroll} ?
                                    delete $options->{uri_param}{scroll} :
                                    '30s';
            }
        }
    }

    # Violate HTTP RFC and set this as the body
    $data = \%params;

    return ($url,$options,$data);
}

sub search_scroll_1_0 {
    my ($url,$options,$data) = @_;

    # If we pass a post 2.0 version of a scroll,
    # translate it back to the 1.0 vesion
    if ( defined $data && is_hashref($data) ) {
        if( exists $data->{scroll} ) {
            $options->{uri_param} ||= {};
            $options->{uri_param}{scroll} = $data->{scroll};
        }
        if( exists $data->{scroll_id} ) {
            my $scroll_id = $data->{scroll_id};
            undef($data);
            $data = $scroll_id;
        }
    }
    return ($url,$options,$data);
}


1;
