# ABSTRACT: Fix version issues to support all the things
package App::ElasticSearch::Utilities::VersionHacks;

use v5.16;
use warnings;
use version;

# VERSION

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
        default => '_forcemerge',
    },
    '_status' => {
        default => '_stats',
    }
);
my %CALLBACKS = (
    '_cluster/state' => {
        default => \&_cluster_state_1_0,
    },
    '_search' => {
        default => \&_search_params,
    },
    '_cat/shards' => {
        default => \&_cat_shards,
    },
);

sub _fix_version_request {
    my ($version,$url,$options,$data) = @_;
    return ($url,$options,$data) unless length $version;

    if (version->parse($version) < $MIN_VERSION) {
        output({stderr=>1,color=>'red',sticky=>1},
                "!!! Detected ElasticSearch Version '$version', which is < $MIN_VERSION, please upgrade your cluster !!!");
        exit 1;
    }

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
                ($url,$options,$data) = $CALLBACKS{$url}->{$version}->($url,$options,$data,$version);
            }
            elsif(exists $CALLBACKS{$url}->{default}) {
                debug({indent=>1,color=>'yellow'}, "+ Callback dispatched for $url by default rule");
                ($url,$options,$data) = $CALLBACKS{$url}->{default}->($url,$options,$data,$version);
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
    my ($url,$options,$data,$version) = @_;

    my @parts = split /\//, $url;

    # Translate old to new
    if( @parts < 3 ) {
        verbose({color=>'yellow'}, "DEPRECATION: Attempting to use legacy API for _cluster/state on ES $version");
        verbose({level=>2,indent=>1}, "See: http://www.elasticsearch.org/guide/en/reference/$version/cluster-state.html#cluster-state");
        my @requested = ();
        if( exists $options->{uri_param} ) {
            my %filters =
                    map  { /filter_(.+)$/; $1 => 1 }
                    grep { /^filter_/ && $options->{uri_param}{$_} }
                keys %{ $options->{uri_param} };
            # Remove them from the parameters
            delete $options->{uri_param}{"filter_$_"} for keys %filters;
            if(keys %filters) {
                foreach my $metric (sort keys %_cluster_state) {
                    push @requested, $metric unless exists $filters{$metric};
                }
            }
        }
        push @requested, '_all' unless @requested;
        push @parts, join(',', @requested);
        my $new_url = join('/',@parts);
        verbose("~ Cluster State rewritten from $url to $new_url");
        $url=$new_url;
    }
    return ($url,$options,$data);
}

sub _search_params {
    my ($url,$options,$data,$version) = @_;

    my @invalid = ();
    push @invalid, "track_total_hits"       if qv($version) < qv("6.0.0");
    push @invalid, "rest_total_hits_as_int" if qv($version) < qv("7.0.0");

    if( @invalid && exists $options->{uri_param} ) {
        foreach my $invalid ( @invalid ) {
            delete $options->{uri_param}{$invalid}
                if exists $options->{uri_param}{$invalid};
        }
    }

    return ($url,$options,$data);
}

sub _cat_shards {
    my ($url,$options,$data,$version) = @_;
    if ( qv($version) >= qv("7.11.0") ) {
        delete $options->{uri_param}{local};
    }
    return ($url,$options,$data);
}

1;
