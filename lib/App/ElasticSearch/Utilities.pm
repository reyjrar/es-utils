# ABSTRACT: Utilities for Monitoring ElasticSearch
package App::ElasticSearch::Utilities;

# VERSION

use strict;
use warnings;

our $ES_CLASS = undef;
our $_OPTIONS_PARSED;
our %_GLOBALS = ();
our @_CONFIGS = (
    '/etc/es-utils.yaml',
    "$ENV{HOME}/.es-utils.yaml",
);

# Because of the poor decision to upload both ElasticSearch and Elasticsearch,
# We need to support both libraries due to some production freezes of ElasticSearch.
{
    if( eval { require Elasticsearch::Compat; 1;} ) {
        $ES_CLASS = "Elasticsearch::Compat";
    }
    elsif( eval { require ElasticSearch; 1; } ) {
        $ES_CLASS = "ElasticSearch";
    }
    else {
        die "Please install Elasticsearch::Compat";
    }
    # Modify the environment
    delete $ENV{$_} for qw(http_proxy https_proxy HTTP_PROXY);
}

use CLI::Helpers qw(:all);
use DateTime;
use Getopt::Long qw(:config pass_through);
use JSON::XS;
use YAML;
use LWP::Simple;
use Sub::Exporter -setup => {
    exports => [ qw(
        es_class
        es_pattern
        es_connect
        es_nodes
        es_indices
        es_indices_meta
        es_index_valid
        es_index_days_old
        es_index_shard_replicas
    )],
    groups => {
        default => [qw(es_connect es_indices)],
        indices => [qw(:default es_indices_meta)],
        index   => [qw(:default es_index_valid es_index_fields es_index_days_old es_index_shard_replicas)],
    },
};

=head1 ARGS

From App::ElasticSearch::Utilities:

    --local         Use localhost as the elasticsearch host
    --host          ElasticSearch host to connect to
    --port          HTTP port for your cluster
    --timeout       Timeout to ElasticSearch, default 30
    --index         Index to run commands against
    --base          For daily indexes, reference only those starting with "logstash"
                     (same as --pattern logstash-* or logstash-DATE)
    --datesep       Date separator, default '.' also (--date-separator)
    --pattern       Use a pattern to operate on the indexes
    --days          If using a pattern or base, how many days back to go, default: all

=head2 ARGUMENT GLOBALS

Some options may be specified in the B</etc/es-utils.yaml> or B<$HOME/.es-utils.yaml> file:

    ---
    host: esproxy.example.com
    port: 80
    timeout: 10

=cut

my %opt = ();
if( !defined $_OPTIONS_PARSED ) {
    GetOptions(\%opt,
        'local',
        'host:s',
        'port:i',
        'timeout:i',
        'index:s',
        'pattern:s',
        'base|index-basename:s',
        'days:i',
        'datesep|date-separator:s',
    );
    $_OPTIONS_PARSED = 1;
}
foreach my $config_file (@_CONFIGS) {
    next unless -f $config_file;
    debug("Loading options from $config_file");
    my %from_file = ();
    eval {
        my $ref = YAML::LoadFile($config_file);
        %from_file = %{$ref};
        debug_var($ref);
    };
    debug({color=>"red"}, "[$config_file] $@") if $@;
    $_GLOBALS{$_} = $from_file{$_} for keys %from_file;
}
# Set defaults
my %DEF = (
    # Connection Options
    HOST        => exists $opt{host} ? $opt{host} :
                   exists $_GLOBALS{host} ? $_GLOBALS{host} :
                   exists $opt{local} ? 'localhost' : 'localhost',
    PORT        => exists $opt{port} ? $opt{port} :
                   exists $_GLOBALS{port} ? $_GLOBALS{port} : 9200,
    TIMEOUT     => exists $opt{timeout} ? $opt{timeout} :
                   exists $_GLOBALS{timeout} ? $_GLOBALS{timeout} : 30,
    # Index selection options
    INDEX       => exists $opt{index} ? $opt{index} : undef,
    BASE        => exists $opt{base} ? lc $opt{base} :
                   exists $opt{'index-basename'} ? lc $opt{'index-basename'} :
                   undef,
    PATTERN     => exists $opt{pattern} ? $opt{pattern} : '*',
    DAYS        => exists $opt{days} ? $opt{days} : undef,
    DATESEP     => exists $opt{datesep} ? $opt{datesep} :
                   exists $opt{'date-separator'} ? lc $opt{'date-separator'} :
                   '.',
);
debug_var(\%DEF);

# Regexes for Pattern Expansion
my %PATTERN_REGEX = (
    '*'  => qr/.*/,
    '?'  => qr/.?/,
    DATE => qr/\d{4}(?:\Q$DEF{DATESEP}\E)?\d{2}(?:\Q$DEF{DATESEP}\E)?\d{2}/,
    ANY  => qr/.*/,
);

if( index($DEF{DATESEP},'-') >= 0 ) {
    output({stderr=>1,color=>'yellow'}, "=== Using a '-' as your date separator may cause problems with other utilities. ===");
}

# Build the Index Pattern
my $PATTERN = $DEF{PATTERN};
foreach my $literal ( keys %PATTERN_REGEX ) {
    $PATTERN =~ s/\Q$literal\E/$PATTERN_REGEX{$literal}/g;
}

=func es_pattern

Returns a hashref of the pattern filter used to get the indexes
    {
        string => '*',
        re     => '.*',
    }

=cut

my %_pattern=(
    re     => $PATTERN,
    string => $DEF{PATTERN},
);
sub es_pattern {
    return wantarray ? %_pattern : \%_pattern;
}

=func es_class

Return the name of the Elasticsearch class implementing our functionality.

=cut

sub es_class {
    return $ES_CLASS;
}

=func es_connect

Without options, this connects to the server defined in the args.  If passed
an array ref, it will use that as the connection definition.

=cut

my $ES = undef;

sub es_connect {
    no strict 'refs';

    if( defined $_[0] && ref $_[0] eq 'ARRAY' ) {
        return $ES_CLASS->new(
            servers    => $_[0],
            transport  => 'http',
            timeout    => $DEF{TIMEOUT},
            no_refresh => 1,
        );
    }

    $ES ||= $ES_CLASS->new(
        servers    => [ "$DEF{HOST}:$DEF{PORT}" ],
        timeout    => $DEF{TIMEOUT},
        transport  => 'http',
        no_refresh => 1,
    );

    return $ES;
}

=func es_nodes

Returns the hash of index meta data.

=cut

my %_nodes;
sub es_nodes {

    if(!keys %_nodes) {
        my $es = es_connect;
        eval {
            my $res = $es->cluster_state(
                filter_nodes         => 0,
                filter_routing_table => 1,
                filter_indices       => 1,
                filter_metadata      => 1,
            );
            die "undefined result from cluster_state()" unless defined $res;
            debug_var($res);
            foreach my $id ( keys %{ $res->{nodes} } ) {
                $_nodes{$id} = $res->{nodes}{$id}{name};
            }
        };
        if ( my $error = $@ ) {
            output({color=>"red"}, "es_nodes(): Unable to locate nodes in status!");
            output({color=>"red"}, $error);
            exit 1;
        }
    }

    return wantarray ? %_nodes : { %_nodes };
}

=func es_indices_meta

Returns the hash of index meta data.

=cut

my $_indices_meta;
sub es_indices_meta {

    if(!defined $_indices_meta) {
        my $es = es_connect;
        eval {
            my $result = $es->cluster_state(
                filter_nodes         => 1,
                filter_routing_table => 1,
                filter_blocks        => 1,
            );
            $_indices_meta = $result->{metadata}{indices};
        };
        if ( !defined $_indices_meta ) {
            output({stderr=>1,color=>"red"}, "es_indices(): Unable to locate indices in status!");
            exit 1;
        }
    }

    my %copy = %{ $_indices_meta };
    return wantarray ? %copy : \%copy;
}

=func es_indices

Returns a list of active indexes matching the filter criteria specified on the command
line.  Can handle indices named:

    logstash-YYYY.MM.DD
    dcid-logstash-YYYY.MM.DD
    logstash-dcid-YYYY.MM.DD
    logstash-YYYY.MM.DD-dcid

Makes use of --datesep to determine where the date is.

=cut

sub es_indices {
    my %args = @_;
    my @indices = ();

    # Simplest case, single index
    if( defined $DEF{INDEX} ) {
        push @indices, $DEF{INDEX} if es_index_valid( $DEF{INDEX} );
    }
    else {
        my %meta = es_indices_meta();
        foreach my $index (keys %meta) {
            debug("Evaluating '$index'");
            if( defined $DEF{BASE} ) {
                debug({indent=>1}, "+ method:base - $DEF{BASE}");
                my @parts = split /\-/, $index;
                my %parts = map { lc($_) => 1 } @parts;
                next unless exists $parts{$DEF{BASE}};
            }
            else {
                my $p = es_pattern;
                debug({indent=>1}, "+ method:patten - $p->{string}");
                next unless $index =~ /^$p->{re}/;
            }
            if( defined $DEF{DAYS} ) {
                debug({indent=>2,color=>"yellow"}, "+ checking to see if index is in the past $DEF{DAYS} days.");

                my $days_old = es_index_days_old( $index );
                unless( defined $days_old && $days_old < $DEF{DAYS} ) {
                    debug({indent=>2,color=>'red'}, "! error locating date in string, skipping !");
                    next;
                }
            }
            debug({indent=>1,color=>"green"}, "+ match!");
            push @indices, $index;
        }
    }

    return @indices;
}

=func es_index_days_old( 'index-name' )

Return the number of days old this index is.

=cut

my $NOW = DateTime->now()->truncate(to => 'day');
sub es_index_days_old {
    my ($index) = @_;

    return unless defined $index;

    if( my ($dateStr) = ($index =~ /($PATTERN_REGEX{DATE})/) ) {
        my @date = split /\Q$DEF{DATESEP}\E/, $dateStr;
        my $idx_dt = DateTime->new( year => $date[0], month => $date[1], day => $date[2] );
        my $duration = $NOW - $idx_dt;
        return $duration->days;
    }
    return;
}

=func es_index_shard_replicas( 'index-name' )

Returns the number of replicas for a given index.

=cut

sub es_index_shard_replicas {
    my ($index) = @_;

    return unless es_index_valid($index);

    my %meta = es_indices_meta();
    return exists $meta{$index} ? $meta{$index}->{settings}{'index.number_of_replicas'} : undef;
}

=func es_index_valid( 'index-name' )

Checks if the specified index is valid

=cut

my %_valid_index = ();
sub es_index_valid {
    my ($index) = @_;

    return unless defined $index && length $index;
    return $_valid_index{$index} if exists $_valid_index{$index};

    my $es = es_connect();

    my $result;
    eval {
        debug("Running index_exists");
        $result = $es->index_exists( index => $index );
        debug_var($result);
    };
    if( defined $result && exists $result->{ok} && $result->{ok} ) {
        return $_valid_index{$index} = 1;
    }
    return $_valid_index{$index} = 0;
}

=func es_index_segments( 'index-name' )

Returns the segment data from the index in a hash or hashref:
    {
        shards   => 3,
        segments => 15,
    }

=cut

sub es_index_segments {
    my ($index) = @_;

    if( !defined $index || !length $index || !es_index_valid($index) ) {
        output({stderr=>1,color=>'red'}, "es_index_segments('$index'): invalid index");
        return undef;
    }

    my $es = es_connect();
    my $result;
    my $rc = eval {
        debug("Fetching segment data.");
        my $json = get( qq{http://$DEF{HOST}:$DEF{PORT}/$index/_segments} );
        my $result = decode_json( $json );
        debug_var($json);
        1;
    };
    if( !$rc || !defined $result ) {
        my $err = $@;
        output({stderr=>1,color=>'red'}, "es_index_segments($index) failed to retrieve segment data", $err);
        return undef;
    }
    my $shard_data = $result->{indices}{$index}{shards};
    my %segments =  map { $_ => 0 } qw(shards segments);
    foreach my $id (keys %{$shard_data} ){
        $segments{segments} += $shard_data->{$id}[0]{num_search_segments};
        $segments{shards}++;
    }

    return wantarray ? %segments : \%segments;

}

=head1 SYNOPSIS

This library contains utilities for unified interfaces in the scripts.

This a set of utilities to make monitoring ElasticSearch clusters much simpler.

Included is:

    scripts/es-status.pl - Command line utility for ES Metrics
    scripts/es-metrics-to-graphite.pl - Send ES Metrics to Graphite or Cacti
    scripts/es-nagios-check.pl - Monitor ES remotely or via NRPE with this script
    scripts/es-daily-index-maintenance.pl - Perform index maintenance on daily indexes
    scripts/es-copy-index.pl - Copy an index from one cluster to another
    scripts/es-alias-manager.pl - Manage index aliases automatically
    scripts/es-apply-settings.pl - Apply settings to all indexes matching a pattern
    scripts/es-storage-data.pl - View how shards/data is aligned on your cluster

The App::ElasticSearch::Utilities module simply serves as a wrapper around the scripts for packaging and
distribution.

=head1 INSTALL

Recommended install with L<CPAN Minus|http://cpanmin.us>:

    cpanm App::ElasticSearch::Utilities

You can also use CPAN:

    cpan App::ElasticSearch::Utilities

Or if you'd prefer to manually install:

    export RELEASE=<CurrentRelease>

    wget --no-check-certificate https://github.com/reyjrar/es-utils/blob/master/releases/App-ElasticSearch-Utilities-$RELEASE.tar.gz?raw=true -O es-utils.tgz

    tar -zxvf es-utils.tgz

    cd App-ElasticSearch-Utilities-$RELEASE

    perl Makefile.PL

    make

    make install

This will take care of ensuring all the dependencies are satisfied and will install the scripts into the same
directory as your Perl executable.

=head2 USAGE

The tools are all wrapped in their own documentation, please see:

    es-status.pl --help
    es-metric-to-graphite.pl --help
    es-nagios-check.pl --help
    es-daily-index-maintenance.pl --help
    es-copy-index.pl --help
    es-alias-manager.pl --help
    es-apply-settings.pl --help
    es-storage-data.pl --help

For individual options and capabilities

=head2 PATTERNS

Patterns are used to match an index to the aliases it should have.  A few symbols are expanded into
regular expressions.  Those patterns are:

    *       expands to match any number of any characters.
    ?       expands to match any single character.
    DATE    expands to match YYYY.MM.DD, YYYY-MM-DD, or YYYYMMDD
    ANY     expands to match any number of any characters.

=head2 CONTRIBUTORS

    Mihai Oprea <mishu@mishulica.com>
    Samit Badle

=cut


1;
