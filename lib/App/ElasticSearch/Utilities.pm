# ABSTRACT: Utilities for Monitoring ElasticSearch
package App::ElasticSearch::Utilities;

# VERSION

use strict;
use warnings;

our $_OPTIONS_PARSED;
our %_GLOBALS = ();
our @_CONFIGS = (
    '/etc/es-utils.yaml',
    '/etc/es-utils.yml',
    "$ENV{HOME}/.es-utils.yaml",
    "$ENV{HOME}/.es-utils.yml",
);

use Elastijk;
use CLI::Helpers qw(:all);
use Getopt::Long qw(:config pass_through);
use Hash::Flatten qw(flatten);
use Hash::Merge::Simple qw(clone_merge);
use Hijk;
use JSON::XS;
use Time::Local;
use URI;
use URI::QueryParam;
use YAML;

use Sub::Exporter -setup => {
    exports => [ qw(
        es_globals
        es_pattern
        es_connect
        es_request
        es_nodes
        es_indices
        es_indices_meta
        es_index_valid
        es_index_bases
        es_index_strip_date
        es_index_days_old
        es_index_shards
        es_index_segments
        es_index_stats
        es_index_fields
        es_settings
        es_node_stats
        es_segment_stats
        es_close_index
        es_open_index
        es_delete_index
        es_optimize_index
        es_apply_index_settings
    )],
    groups => {
        config  => [qw(es_globals)],
        default => [qw(es_connect es_indices es_request)],
        indices => [qw(:default es_indices_meta)],
        index   => [qw(:default es_index_valid es_index_fields es_index_days_old es_index_bases)],
    },
};
use App::ElasticSearch::Utilities::VersionHacks qw(_fix_version_request);

=head1 ARGS

From App::ElasticSearch::Utilities:

    --local         Use localhost as the elasticsearch host
    --host          ElasticSearch host to connect to
    --port          HTTP port for your cluster
    --noop          Any operations other than GET are disabled, can be negated with --no-noop
    --timeout       Timeout to ElasticSearch, default 30
    --keep-proxy    Do not remove any proxy settings from %ENV
    --index         Index to run commands against
    --base          For daily indexes, reference only those starting with "logstash"
                     (same as --pattern logstash-* or logstash-DATE)
    --datesep       Date separator, default '.' also (--date-separator)
    --pattern       Use a pattern to operate on the indexes
    --days          If using a pattern or base, how many days back to go, default: all

=head1 ARGUMENT GLOBALS

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
        'keep-proxy',
        'index:s',
        'pattern:s',
        'base|index-basename:s',
        'days:i',
        'noop!',
        'datesep|date-separator:s',
    );
    $_OPTIONS_PARSED = 1;
}
my @ConfigData=();
foreach my $config_file (@_CONFIGS) {
    next unless -f $config_file;
    debug("Loading options from $config_file");
    eval {
        my $ref = YAML::LoadFile($config_file);
        push @ConfigData, $ref;
        debug_var($ref);
        1;
    } or do {
        debug({color=>"red"}, "[$config_file] $@");
    };
}
%_GLOBALS  = @ConfigData ? %{ clone_merge(@ConfigData) } : ();

# Set defaults
my %DEF = (
    # Connection Options
    HOST        => exists $opt{host}              ? $opt{host} :
                   exists $opt{local}             ? 'localhost' :
                   exists $_GLOBALS{host}         ? $_GLOBALS{host} : 'localhost',
    PORT        => exists $opt{port}              ? $opt{port} :
                   exists $_GLOBALS{port}         ? $_GLOBALS{port} : 9200,
    TIMEOUT     => exists $opt{timeout}           ? $opt{timeout} :
                   exists $_GLOBALS{timeout}      ? $_GLOBALS{timeout} : 30,
    NOOP        => exists $opt{noop}              ? $opt{noop} :
                   exists $_GLOBALS{noop}         ? $_GLOBALS{noop} : undef,
    NOPROXY     => exists $opt{'keep-proxy'}      ? 0 :
                   exists $_GLOBALS{'keep-proxy'} ? $_GLOBALS{'keep-proxy'} : 1,
    # Index selection options
    INDEX       => exists $opt{index}     ? $opt{index} : undef,
    BASE        => exists $opt{base}      ? lc $opt{base} :
                   exists $_GLOBALS{base} ? $_GLOBALS{base} : undef,
    PATTERN     => exists $opt{pattern}   ? $opt{pattern} : '*',
    DAYS        => exists $opt{days}      ? $opt{days} :
                   exists $_GLOBALS{days} ? $_GLOBALS{days} : 7,
    DATESEP     => exists $opt{datesep}               ? $opt{datesep} :
                   exists $_GLOBALS{datesep}          ? $_GLOBALS{datesep} :
                   exists $_GLOBALS{"date-separator"} ? $_GLOBALS{"date-separator"} :
                   '.',
);
debug_var(\%DEF);
CLI::Helpers::override(verbose => 1) if $DEF{NOOP};

if( $DEF{NOPROXY} ) {
    debug("Removing any active HTTP Proxies from ENV.");
    delete $ENV{$_} for qw(http_proxy HTTP_PROXY);
}

# Regexes for Pattern Expansion
my %PATTERN_REGEX = (
    '*'  => qr/.*/,
    DATE => qr/\d{4}(?:\Q$DEF{DATESEP}\E)?\d{2}(?:\Q$DEF{DATESEP}\E)?\d{2}/,
    ANY  => qr/.*/,
);
my @ORDERED = qw(* DATE ANY);

if( index($DEF{DATESEP},'-') >= 0 ) {
    output({stderr=>1,color=>'yellow'}, "=== Using a '-' as your date separator may cause problems with other utilities. ===");
}

# Build the Index Pattern
my $PATTERN = $DEF{PATTERN};
foreach my $literal ( @ORDERED ) {
    $PATTERN =~ s/\Q$literal\E/$PATTERN_REGEX{$literal}/g;
}

our $CURRENT_VERSION;

=func es_globals($key)

Grab the value of the global value from the es-utils.yaml files.

=cut

sub es_globals {
    my ($key) = @_;

    return unless exists $_GLOBALS{$key};
    return $_GLOBALS{$key};
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

sub _get_es_version {
    return $CURRENT_VERSION if defined $CURRENT_VERSION;
    eval {
        my ($status,$result) = Elastijk::request({
            host    => $DEF{HOST},
            port    => $DEF{PORT},
            method  => 'GET',
            command => '/',
        });
        if( $status eq "200" ) {
            $CURRENT_VERSION = join('.', (split /\./,$result->{version}{number})[0,1]);
        }
    };
    if( !defined $CURRENT_VERSION || $CURRENT_VERSION <= 0 ) {
        output({color=>'red',stderr=>1}, "Unable to determine Elasticsearch version, something has gone terribly wrong: aborting.");
        exit 1;
    }
    debug({color=>'magenta'}, "FOUND VERISON '$CURRENT_VERSION'");
    return $CURRENT_VERSION;
};

=func es_connect

Without options, this connects to the server defined in the args.  If passed
an array ref, it will use that as the connection definition.

=cut

my $ES = undef;

sub es_connect {
    my ($override_servers) = @_;

    my $server = $DEF{HOST};
    my $port   = $DEF{PORT};

    # If we're overriding, return a unique handle
    if(defined $override_servers) {
        my @overrides = ref $override_servers eq 'ARRAY' ? @$override_servers : $override_servers;
        my @servers;
        foreach my $entry ( @overrides ) {
            my ($s,$p) = split /\:/, $entry;
            $p ||= $port;
            push @servers, { host => $s, port => $p };
        }

        if( @servers > 0 ) {
            my $pick = @servers > 1 ? $servers[int(rand(@servers))] : $servers[0];
            return Elastijk->new(%{$pick});
        }
    }

    # Otherwise, cache our handle
    $ES ||= Elastijk->new(
        host => $server,
        port => $port
    );

    return $ES;
}

=func es_request([$handle],$command,{ method => 'GET', uri_param => { a => 1 } }, {})

Retrieve URL from ElasticSearch, returns a hash reference

First hash ref contains options, including:

    uri_param           Query String Parameters
    index               Index name
    type                Index type
    method              Default is GET

=cut

sub es_request {
    my $instance = ref $_[0] eq 'Elastijk::oo' ? shift @_ : es_connect();

    $CURRENT_VERSION = _get_es_version() if !defined $CURRENT_VERSION;

    my($url,$options,$body) = _fix_version_request(@_);

    # Pull connection options
    $options->{$_} = $instance->{$_} for qw(host port);

    $options->{method} ||= 'GET';
    $options->{body} = $body if defined $body;
    $options->{command} = $url;
    my $index = 'NoIndex';

    if( exists $options->{index} ) {
        my $index_in = delete $options->{index};
        #
        # No need to validate _all
        if( $index_in eq '_all') {
            $index = $index_in;
        }
        else {
            # Validate each included index
            my @valid;
            my @test = ref $index_in eq 'ARRAY' ? @{ $index_in } : split /\,/, $index_in;
            foreach my $i (@test) {
                push @valid, $i if es_index_valid($i);
            }
            $index = join(',', @valid);
        }
    }
    $options->{index} = $index if $index ne 'NoIndex';

    # Figure out if we're modifying things
    my $modification = $url eq '_search' && $options->{method} eq 'POST' ? 0
                     : $options->{method} ne 'GET';

    my ($status,$res);
    if( $DEF{NOOP} && $modification) {
        output({color=>'cyan'}, "Called es_request($index / $options->{command}), but --noop and method is $options->{method}");
        return;
    }
    # _cat Queries don't work with Elastijk
    if( $url =~ m#^_cat/# ) {
        my $uri = URI->new(sprintf "http://%s:%d/%s", $options->{host}, $options->{port}, $url);
        # Merge URI keys and uri_param keys
        if ( exists $options->{uri_param} ) {
            foreach my $k (keys %{ $options->{uri_param} }) {
                $uri->query_param( $k => $options->{uri_param}{$k} );
            }
        }
        eval {
            my $resp = Hijk::request({
                host            => $uri->host,
                port            => $uri->port,
                method          => 'GET',
                path            => $uri->path,
                query_string    => $uri->query,
                connect_timeout => $DEF{timeout},
                read_timeout    => $DEF{timeout},
            });
            $status = $resp->{status};
            die sprintf "%d - %s", $resp->{error_number}, $resp->{error_string} unless $status == 200;
            $res = [ map { s/^\s+//; s/\s+$//; $_ } grep { defined && length } split /\r?\n/, $resp->{body} ];
            1;
        } or do {
            output({color=>'red',stderr=>1},sprintf "es_request() hijk::request(%s?%s) failed: %s", $uri->path, $uri->query, $@);
        };
    }
    else {
        eval {
            debug("calling es_request($index / $options->{command})");
            ($status,$res) = Elastijk::request($options);
            die sprintf "[%d] Empty response from %s:%d", $status, $options->{host}, $options->{port} unless defined $res;
            1;
        } or do {
            my $err = $@;
            output({color=>'red',stderr=>1}, "es_request($index / $options->{command}) failed[$status]: $err");
        };
    }
    verbose({color=>'yellow'},"es_request($index / $options->{command}) returned HTTP Status $status") if $status != 200;

    return $res;
}


=func es_nodes

Returns the hash of index meta data.

=cut

my %_nodes;
sub es_nodes {
    if(!keys %_nodes) {
        my $res = es_request('_cluster/state/nodes', {});
        if( !defined $res  ) {
            output({color=>"red"}, "es_nodes(): Unable to locate nodes in status!");
            exit 1;
        }
        debug_var($res);
        foreach my $id ( keys %{ $res->{nodes} } ) {
            $_nodes{$id} = $res->{nodes}{$id}{name};
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
        my $result = es_request('_cluster/state/metadata');
        $_indices_meta = $result->{metadata}{indices};
        if ( !defined $_indices_meta ) {
            output({stderr=>1,color=>"red"}, "es_indices_meta(): Unable to locate indices in status!");
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

Options include:

=over 4

=item B<state>

Default is 'open', can be used to find 'closed' indexes as well.

=item B<check_state>

Default is 1, set to 0 to disable state checks.  The combination of the default
with this option and the default for B<state> means only open indices are returned.

=item B<check_dates>

Default is 1, set to 0 to disable checking index age.

=back

=cut

my %_valid_index = ();
sub es_indices {
    my %args = (
        state       => 'open',
        check_state => 1,
        check_dates => 1,
        @_
    );
    # Seriously, English? Do you speak it motherfucker?
    $args{state} = 'close' if $args{state} eq 'closed';

    my @indices = ();

    # Simplest case, single index
    if( defined $DEF{INDEX} ) {
        push @indices, $DEF{INDEX} if es_index_valid( $DEF{INDEX} );
    }
    else {
        my $res = es_request('_cat/indices?h=index,status');
        foreach my $line (@{ $res }) {
            my ($index,$status) = split /\s+/, $line;
            debug("Evaluating '$index'");
            if(!exists $args{_all}) {
                # State Check Disqualification
                if($args{state} ne 'all'  && $args{check_state})  {
                    my $result = $status eq $args{state};
                    debug({indent=>1,color=>$result ? 'green' : 'red' },
                        sprintf('+ method:state=%s, got %s', $args{state}, $status)
                    );
                    next unless $result;
                }

                if( defined $DEF{BASE} ) {
                    debug({indent=>1}, "+ method:base - $DEF{BASE}");
                    my %bases = map { $_ => 1 } es_index_bases($index);
                    next unless exists $bases{$DEF{BASE}};
                }
                else {
                    my $p = es_pattern;
                    debug({indent=>1}, sprintf "+ method:pattern - %s", encode_json($p));
                    next unless $index =~ /$p->{re}/;
                }
                debug({indent=>2},"= name checks succeeded");
                if( $args{check_dates} && defined $DEF{DAYS} ) {
                    debug({indent=>2,color=>"yellow"}, "+ checking to see if index is in the past $DEF{DAYS} days.");

                    my $days_old = es_index_days_old( $index );
                    debug(sprintf "%s is %s days old", $index, defined $days_old ? $days_old : 'undef');
                    if( !defined $days_old ) {
                        debug({indent=>2,color=>'red'}, "! error locating date in string, skipping !");
                        next;
                    }
                    elsif( $DEF{DAYS} >= 0 && $days_old >= $DEF{DAYS} ) {
                        next;
                    }
                }
            }
            else {
                debug({indent=>1}, "Called with _all, all checks skipped.");
            }
            debug({indent=>1,color=>"green"}, "+ match!");
            push @indices, $index;
        }
    }

    # We retrieved these from the cluster, so preserve them here.
    $_valid_index{$_} = 1 for @indices;

    return wantarray ? @indices : \@indices;
}

=func es_index_strip_date( 'index-name' )

Returns the index name with the date removed.

=cut

sub es_index_strip_date {
    my ($index) = @_;

    return -1 unless defined $index;

    if( $index =~ s/[-_]$PATTERN_REGEX{DATE}// ) {
        return $index;
    }
    return undef;
}

=func es_index_bases( 'index-name' )

Returns an array of the possible index base names for this index

=cut

my %_stripped=();

sub es_index_bases {
    my ($index) = @_;

    return unless defined $index;

    # Strip to the base
    my $stripped = es_index_strip_date($index);
    return unless defined $stripped and length $stripped;

    # Compute if we haven't already memoized
    if( !exists $_stripped{$stripped} ) {
        my %bases=();
        my @parts = grep { defined && length } split /[-_]/, $stripped;
        debug(sprintf "es_index_bases(%s) dissected to %s", $index, join(',', @parts));
        my $sep = index( $stripped, '_' ) >= 0 ? '_' : '-';

        my %collected = ();
        while( my $word = shift @parts ) {
            my @set=($word);
            $collected{$word} =1;
            foreach my $sub ( @parts ) {
                push @set, $sub;
                $collected{join($sep,@set)} =1;
            }
        }
        $_stripped{$stripped} = [ sort keys %collected ]
    }

    return @{ $_stripped{$stripped} };
}

=func es_index_days_old( 'index-name' )

Return the number of days old this index is.

=cut

my $NOW = timegm(0,0,0,(gmtime)[3,4,5]);
sub es_index_days_old {
    my ($index) = @_;

    return unless defined $index;

    if( my ($dateStr) = ($index =~ /($PATTERN_REGEX{DATE})/) ) {
        my @date=();
        if(length $DEF{DATESEP}) {
           @date = reverse map { int } split /\Q$DEF{DATESEP}\E/, $dateStr;
        }
        else {
            for my $len (qw(4 2 2)) {
                unshift @date, substr($dateStr,0,$len,'');
            }
        }
        $date[1]--; # move 1-12 -> 0-11
        my $idx_time = timegm( 0,0,0, @date );
        my $diff = $NOW - $idx_time;
        $diff++;    # Add one second
        debug({color=>"yellow"}, sprintf "es_index_days_old(%s) - Time difference is %0.3f", $index, $diff/86400);
        return int($diff / 86400);
    }
    return;
}


=func es_index_shard_replicas( 'index-name' )

Returns the number of replicas for a given index.

=cut

sub es_index_shards {
    my ($index) = @_;

    my %shards = map { $_ => 0 } qw(primaries replicas);
    my $result = es_request('_settings', {index=>$index});
    if( defined $result && ref $result eq 'HASH')  {
        $shards{primaries} = $CURRENT_VERSION < 1.0 ? $result->{$index}{settings}{'index.number_of_shards'}
                                                    : $result->{$index}{settings}{index}{number_of_shards};
        $shards{replicas}  = $CURRENT_VERSION < 1.0 ? $result->{$index}{settings}{'index.number_of_replicas'}
                                                    : $result->{$index}{settings}{index}{number_of_replicas};
    }

    return wantarray ? %shards : \%shards;
}

=func es_index_valid( 'index-name' )

Checks if the specified index is valid

=cut

sub es_index_valid {
    my ($index) = @_;

    return unless defined $index && length $index;
    return $_valid_index{$index} if exists $_valid_index{$index};

    my $es = es_connect();

    my $result;
    eval {
        debug("Running index_exists");
        $result = $es->exists( index => $index );
    };
    return $_valid_index{$index} = $result;
}

=func es_index_fields('index-name')

Returns a list of the fields in a given index.

=cut

sub es_index_fields {
    my ($index) = @_;

    my $result = es_request('_mapping', { index => $index });

    # Handle Version incompatibilities
    my $ref = exists $result->{$index}{mappings} ? $result->{$index}{mappings} : $result->{$index};

    # Loop through the mappings, skipping _default_
    my @mappings = grep { $_ ne '_default_' } keys %{ $ref };
    my %fields;
    foreach my $mapping (@mappings) {
        _find_fields(\%fields,$ref->{$mapping});
    }
    my @uniq = sort grep { $fields{$_} == 1 } keys %fields;
    return wantarray ? @uniq : \@uniq;
}

sub _add_fields {
    my ($f,@path) = @_;
    my $key = join('.', @path);
    $f->{$key} ||= 0;
    $f->{$key}++;
    if( $key ne $path[-1] ) {
        $f->{$path[-1]} ||= 0;
        $f->{$path[-1]}++;
    }
}

sub _find_fields {
    my ($f,$ref,@path) = @_;

    if( exists $ref->{type} ) {
        _add_fields($f,@path);
    }
    if( exists $ref->{properties} && ref $ref->{properties} eq 'HASH') {
        foreach my $k (sort keys %{ $ref->{properties} }) {
            _find_fields($f,$ref->{properties}{$k},@path,$k);
        }
    }
    if( exists $ref->{fields} && ref $ref->{fields} eq 'HASH') {
        foreach my $k (sort keys %{ $ref->{fields} } ) {
            _add_fields($f,@path,$k);
        }
    }
}

=func es_close_index('index-name')

Closes an index

=cut

sub es_close_index {
    my($index) = @_;

    return es_request('_close',{ method => 'POST', index => $index });
}

=func es_open_index('index-name')

Open an index

=cut

sub es_open_index {
    my($index) = @_;

    return es_request('_open',{ method => 'POST', index => $index });
}

=func es_delete_index('index-name')

Deletes an index

=cut

sub es_delete_index {
    my($index) = @_;

    return es_request('',{ method => 'DELETE', index => $index });
}

=func es_optimize_index('index-name')

Optimize an index to a single segment per shard

=cut

sub es_optimize_index {
    my($index) = @_;

    return es_request('_optimize',{
            method    => 'POST',
            index     => $index,
            uri_param => {
                max_num_segments => 1,
                wait_for_merge   => 0,
            },
    });
}

sub es_apply_index_settings {
    my($index,$settings) = @_;

    if(ref $settings ne 'HASH') {
        output({stderr=>1,color=>'red'}, 'usage is es_apply_index_settings($index,$settings_hashref)');
        return;
    }

    return es_request('_settings',{ method => 'PUT', index => $index },$settings);
}

=func es_index_segments( 'index-name' )

Exposes GET /$index/_segments

Returns the segment data from the index in hashref:

=cut

sub es_index_segments {
    my ($index) = @_;

    if( !defined $index || !length $index || !es_index_valid($index) ) {
        output({stderr=>1,color=>'red'}, "es_index_segments('$index'): invalid index");
        return undef;
    }

    return es_request('_segments', {
        index => $index,
    });

}

=func es_segment_stats($index)

Return the number of shards and segments in an index as a hashref

=cut

sub es_segment_stats {
    my ($index) = @_;

    my %segments =  map { $_ => 0 } qw(shards segments);
    my $result = es_index_segments($index);

    if(defined $result) {
        my $shard_data = $result->{indices}{$index}{shards};
        foreach my $id (keys %{$shard_data}) {
            $segments{segments} += $shard_data->{$id}[0]{num_search_segments};
            $segments{shards}++;
        }
    }
    return wantarray ? %segments : \%segments;
}


=func es_index_stats( 'index-name' )

Exposes GET /$index/_stats

Returns a hashref

=cut

sub es_index_stats {
    my ($index) = @_;

    return es_request('_stats', {
        index     => $index,
        uri_param => { all => 'true' },
    });
}


=func es_settings()

Exposes GET /_settings

Returns a hashref

=cut

sub es_settings {
    return es_request('_settings');
}

=func es_node_stats()

Exposes GET /_nodes/stats

Returns a hashref

=cut

sub es_node_stats {
    my (@nodes) = @_;

    my @cmd = qw(_nodes);
    push @cmd, join(',', @nodes) if @nodes;
    push @cmd, 'stats';

    return es_request(join('/',@cmd), { uri_param => {all => 'true', human => 'true'} });
}

=func def('key')

Exposes Definitions grabbed by options parsing

=cut

sub def {
    my($key)= map { uc }@_;

    return exists $DEF{$key} ? $DEF{$key} : undef;
}


=head1 SYNOPSIS

This library contains utilities for unified interfaces in the scripts.

This a set of utilities to make monitoring ElasticSearch clusters much simpler.

Included are:

B<SEARCHING>:

    scripts/es-search.pl - Utility to interact with LogStash style indices from the CLI

B<MONITORING>:

    scripts/es-nagios-check.pl - Monitor ES remotely or via NRPE with this script
    scripts/es-graphite-dynamic.pl - Perform index maintenance on daily indexes
    scripts/es-status.pl - Command line utility for ES Metrics
    scripts/es-storage-data.pl - View how shards/data is aligned on your cluster
    scripts/es-nodes.pl - View node information

B<MAINTENANCE>:

    scripts/es-daily-index-maintenance.pl - Perform index maintenance on daily indexes
    scripts/es-alias-manager.pl - Manage index aliases automatically
    scripts/es-open.pl - Open any closed indices matching a index parameters

B<MANAGEMENT>:

    scripts/es-copy-index.pl - Copy an index from one cluster to another
    scripts/es-apply-settings.pl - Apply settings to all indexes matching a pattern
    scripts/es-storage-data.pl - View how shards/data is aligned on your cluster

B<DEPRECATED>:

    scripts/es-graphite-static.pl - Send ES Metrics to Graphite or Cacti

The App::ElasticSearch::Utilities module simply serves as a wrapper around the scripts for packaging and
distribution.

=head1 INSTALL

B<This library attempts to provide scripts compatible with version 0.19 through 1.1 of ElasticSearch>.

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

    $UTILITY --help
    $UTILITY --manual

For individual options and capabilities

=head2 PATTERNS

Patterns are used to match an index to the aliases it should have.  A few symbols are expanded into
regular expressions.  Those patterns are:

    *       expands to match any number of any characters.
    DATE    expands to match YYYY.MM.DD, YYYY-MM-DD, or YYYYMMDD
    ANY     expands to match any number of any characters.

=cut


1;
