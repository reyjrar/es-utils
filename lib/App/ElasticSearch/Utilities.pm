# ABSTRACT: Utilities for Monitoring ElasticSearch
package App::ElasticSearch::Utilities;

use v5.16;
use warnings;

# VERSION

use App::ElasticSearch::Utilities::HTTPRequest;
use CLI::Helpers qw(:all);
use Getopt::Long qw(GetOptionsFromArray :config pass_through no_auto_abbrev);
use Hash::Flatten qw(flatten);
use Hash::Merge::Simple qw(clone_merge);
use IPC::Run3;
use JSON::MaybeXS;
use LWP::UserAgent;
use Net::Netrc;
use Ref::Util qw(is_ref is_arrayref is_hashref);
use Time::Local;
use URI;
use URI::QueryParam;
use YAML::XS ();

# Control loading ARGV
my $ARGV_AT_INIT    = 1;
my $COPY_ARGV       = 0;
our $_init_complete = 0;

use Sub::Exporter -setup => {
    collectors => [
        copy_argv       => \'_copy_argv',
        preprocess_argv => \'_preprocess_argv',
        delay_argv      => \'_delay_argv',
    ],
    exports => [ qw(
        es_utils_initialize
        es_globals
        es_basic_auth
        es_pattern
        es_connect
        es_master
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
        es_local_index_meta
        es_flatten_hash
        es_human_count
        es_human_size
        es_format_numeric
    )],
    groups => {
        config  => [qw(es_utils_initialize es_globals)],
        default => [qw(es_utils_initialize es_connect es_indices es_request)],
        human   => [qw(es_human_count es_human_size es_format_numeric)],
        indices => [qw(:default es_indices_meta)],
        index   => [qw(
            :default es_index_valid es_index_fields es_index_days_old es_index_bases
            es_index_strip_date es_index_shards es_index_segments es_index_stats
        )],
        maintenance => [qw(
            es_close_index es_open_index es_delete_index es_optimize_index
            es_apply_index_settings
        )],
    },
};

=head1 OVERVIEW

In addition to the scripts, the libraries provide a simplistic interface to
write your own scripts. It builds C<CLI::Helpers> to provide consistent options
for scripts.

    use App::ElasticSearch::Utilities qw(:all);
    use Data::Printer;

    my $res = es_result('_cluster/health');
    p($res)

See the contents of the scripts for examples.

=head1 EXPORT

This module use L<Sub::Exporter> so you can customize exports.

=head2 Export Groups

The following export groups are provided.

=over 2

=item B<:default> - Default exports

    es_connect()
    es_indices()
    es_request()
    es_utils_initialize()

=item B<:config>

    es_globals()
    es_utils_initialize()

=item B<:human>

    es_format_numeric()
    es_human_count()
    es_human_size()

=item B<:index>

    :default
    es_index_bases()
    es_index_days_old()
    es_index_fields()
    es_index_shards()
    es_index_segments()
    es_index_stats()
    es_index_strip_date()
    es_index_valid()

=item B<:indices>

    :default
    es_indices_meta()

=item B<:maintenance>

    :default
    es_close_index()
    es_open_index()
    es_delete_index()
    es_optimize_index()
    es_apply_index_settings()

=item B<:all> - All exportable functions

    :config
    :default
    :index
    :indices
    :human
    :maintenance
    es_basic_auth()
    es_flatten_hash()
    es_local_index_meta()
    es_master()
    es_nodes()
    es_node_stats()
    es_pattern()
    es_settings()
    es_segment_stats()

=back

=head2 Configuration

It is possible to control how and when C<@ARGV> is processed to prevent conflicts.


=cut

use App::ElasticSearch::Utilities::Connection;
use App::ElasticSearch::Utilities::VersionHacks qw(_fix_version_request);

# Collectors
sub _copy_argv       { $COPY_ARGV    = 1 }
sub _preprocess_argv { $ARGV_AT_INIT = 1 }
sub _delay_argv      { $ARGV_AT_INIT = 0 }

=head1 ARGS

From App::ElasticSearch::Utilities:

    --local         Use localhost as the elasticsearch host
    --host          ElasticSearch host to connect to
    --port          HTTP port for your cluster
    --proto         Defaults to 'http', can also be 'https'
    --http-username HTTP Basic Auth username
    --password-exec Script to run to get the users password
    --insecure      Don't verify TLS certificates
    --cacert        Specify the TLS CA file
    --capath        Specify the directory with TLS CAs
    --cert          Specify the path to the client certificate
    --key           Specify the path to the client private key file
    --noop          Any operations other than GET are disabled, can be negated with --no-noop
    --timeout       Timeout to ElasticSearch, default 10
    --keep-proxy    Do not remove any proxy settings from %ENV
    --index         Index to run commands against
    --base          For daily indexes, reference only those starting with "logstash"
                     (same as --pattern logstash-* or logstash-DATE)
    --pattern       Use a pattern to operate on the indexes
    --days          If using a pattern or base, how many days back to go, default: 1

See also the "CONNECTION ARGUMENTS" and "INDEX SELECTION ARGUMENTS" sections from App::ElasticSearch::Utilities.

=head1 CONFIG FILES

Some options may be specified in the B</etc/es-utils.yaml>, B<$HOME/.es-utils.yaml>
or B<$HOME/.config/es-utils/config.yaml> file:

    ---
    base: logstash
    days: 7
    host: esproxy.example.com
    port: 80
    timeout: 10
    proto: https
    http-username: bob
    password-exec: /home/bob/bin/get-es-passwd.sh

=head1 CONNECTION ARGUMENTS

Arguments for establishing a connection with the cluster.  Unless specified otherwise, these options
can all be set in the globals file.

=over

=item B<local>

Assume ElasticSearch is running locally, connect to localhost.

=item B<host>

Use a different hostname or IP address to connect.

=item B<port>

Defaults to 9200.

=item B<proto>

Defaults to 'http', can also be 'https'.

=item B<http-username>

If HTTP Basic Authentication is required, use this username.

See also the L<HTTP Basic Authentication> section for more details

=item B<password-exec>

If HTTP Basic Authentication is required, run this command, passing the arguments:

    <command_to_run> <es_host> <es_username>

The script expects the last line to contain the password in plaintext.

=item B<noop>

Prevents any communication to the cluster from making changes to the settings or data contained therein.
In short, it prevents anything but HEAD and GET requests, B<except> POST requests to the _search endpoint.

=item B<timeout>

Timeout for connections and requests, defaults to 10.

=item B<keep-proxy>

By default, HTTP proxy environment variables are stripped. Use this option to keep your proxy environment variables
in tact.

=item B<insecure>

Don't verify TLS certificates

=item B<cacert>

Specify a file with the TLS CA certificates.

=item B<capath>

Specify a directory containing the TLS CA certificates.

=item B<cert>

Specify the path to the TLS client certificate file..

=item B<key>

Specify the path to the TLS client private key file.

=back

=head1 INDEX SELECTION ARGUMENTS

=over

=item B<base>

In an environment using monthly, weekly, daily, or hourly indexes.  The base index name is everything without the date.
Parsing for bases, also provides splitting and matching on segments of the index name delineated by the '-' character.
If we have the following indexes:

    web-dc1-YYYY.MM.DD
    web-dc2-YYYY.MM.DD
    logstash-dc1-YYYY.MM.DD
    logstash-dc2-YYYY.MM.DD

Valid bases would be:

    web
    web-dc1
    web-dc2
    logstash
    logstash-dc1
    logstash-dc2
    dc1
    dc2

Combining that with the days option can provide a way to select many indexes at once.

=item B<days>

How many days backwards you want your operation to be relevant.

=item B<datesep>

Default is '.' Can be set to an empty string for no separator.

=item B<pattern>

A pattern to match the indexes.  Can expand the following key words and characters:

    '*'    expanded to '.*'
    'ANY'  expanded to '.*'
    'DATE' expanded to a pattern to match a date,

The indexes are compared against this pattern.

=back

=cut

# Global Variables
our %_GLOBALS = ();
my  %DEF      = ();
my  %PATTERN_REGEX = (
    '*'  => qr/.*/,
    ANY  => qr/.*/,
    DATE => qr/
        (?<datestr>
                (?<year>\d{4})              # Extract 4 digits for the year
                (?:(?<datesep>[\-.]))?      # Optionally, look for . - as a separator
                (?<month>\d{2})             # Two digits for the month
                \g{datesep}                 # Whatever the date separator was in the previous match
                (?<day>\d{2})               # Two digits for the day
                (?![a-zA-Z0-9])             # Zero width negative look ahead, not alphanumeric
        )
    /x,
);
my $PATTERN;

{
    ## no critic (ProhibitNoWarnings)
    no warnings;
    INIT {
        return if $_init_complete++;
        es_utils_initialize() if $ARGV_AT_INIT;
    }
    ## use critic
}


{
    # Argument Parsing Block
    my @argv_original = ();
    my $parsed_argv = 0;
    sub _parse_options {
        my ($opt_ref) = @_;
        my @opt_spec = qw(
            local
            host=s
            port=i
            timeout=i
            keep-proxy
            index=s
            pattern=s
            base|index-basename=s
            days=i
            noop!
            proto=s
            http-username=s
            password-exec=s
            master-only|M
            insecure
            capath=s
            cacert=s
            cert=s
            key=s
        );

        my $argv;
        my %opt;
        if( defined $opt_ref && is_arrayref($opt_ref) ) {
            # If passed an argv array, use that
            $argv = $COPY_ARGV ? [ @{ $opt_ref } ] : $opt_ref;
        }
        else {
            # Ensure multiple calls to cli_helpers_initialize() yield the same results
            if ( $parsed_argv ) {
                ## no critic
                @ARGV = @argv_original;
                ## use critic
            }
            else {
                @argv_original = @ARGV;
                $parsed_argv++;
            }
            # Operate on @ARGV
            $argv = $COPY_ARGV ? [ @ARGV ] : \@ARGV;
        }
        GetOptionsFromArray($argv, \%opt, @opt_spec );
        return \%opt;
    }
}

=config es_utils_initialize()

Takes an optional reference to an C<@ARGV> like array. Performs environment and
argument parsing.

=cut

sub es_utils_initialize {
    my ($argv) = @_;

    # Parse Options
    my $opts = _parse_options($argv);

    # Config file locations
    my @configs = (
        '/etc/es-utils.yaml',
        '/etc/es-utils.yml',
    );
    if( $ENV{HOME} ) {
        push @configs, map { "$ENV{HOME}/.es-utils.$_" } qw( yaml yml );
        my $xdg_config_home = $ENV{XDG_CONFIG_HOME} || "$ENV{HOME}/.config";
        push @configs, map { "${xdg_config_home}/es-utils/config.$_" } qw( yaml yml );
    }

    my @ConfigData=();
    foreach my $config_file (@configs) {
        next unless -f $config_file;
        debug("Loading options from $config_file");
        eval {
            my $ref = YAML::XS::LoadFile($config_file);
            push @ConfigData, $ref;
            debug_var($ref);
            1;
        } or do {
            debug({color=>"red"}, "[$config_file] $@");
        };
    }
    %_GLOBALS  = @ConfigData ? %{ clone_merge(@ConfigData) } : ();

    # Set defaults
    %DEF = (
        # Connection Options
        HOST        => exists $opts->{host}   ? $opts->{host}
                     : exists $opts->{local}  ? 'localhost'
                     : exists $_GLOBALS{host} ? $_GLOBALS{host}
                     : 'localhost',
        PORT        => exists $opts->{port}   ? $opts->{port}
                     : exists $_GLOBALS{port} ? $_GLOBALS{port}
                     : 9200,
        PROTO       => exists $opts->{proto}   ? $opts->{proto}
                     : exists $_GLOBALS{proto} ? $_GLOBALS{proto}
                     : 'http',
        TIMEOUT     => exists $opts->{timeout}   ? $opts->{timeout}
                     : exists $_GLOBALS{timeout} ? $_GLOBALS{timeout}
                     : 10,
        NOOP        => exists $opts->{noop}   ? $opts->{noop}
                     : exists $_GLOBALS{noop} ? $_GLOBALS{noop}
                     : undef,
        NOPROXY     => exists $opts->{'keep-proxy'}   ? 0
                     : exists $_GLOBALS{'keep-proxy'} ? $_GLOBALS{'keep-proxy'}
                     : 1,
        MASTERONLY  => exists $opts->{'master-only'} ? $opts->{'master-only'} : 0,
        # Index selection opts->ions
        INDEX       => exists $opts->{index}  ? $opts->{index} : undef,
        BASE        => exists $opts->{base}   ? lc $opts->{base}
                     : exists $_GLOBALS{base} ? $_GLOBALS{base}
                     : undef,
        PATTERN     => exists $opts->{pattern} ? $opts->{pattern} : '*',
        DAYS        => exists $opts->{days}    ? $opts->{days}
                     : exists $_GLOBALS{days}  ? $_GLOBALS{days} : 1,
        # HTTP Basic Authentication
        USERNAME    => exists $opts->{'http-username'}    ? $opts->{'http-username'}
                     : exists $_GLOBALS{'http-username'}  ? $_GLOBALS{'http-username'}
                     : $ENV{USER},
        PASSEXEC    => exists $opts->{'password-exec'}   ? $opts->{'password-exec'}
                     : exists $_GLOBALS{'password-exec'} ? $_GLOBALS{'password-exec'}
                     : undef,
        # TLS Options
        INSECURE    => exists $opts->{insecure} ? 1
                    :  exists $_GLOBALS{insecure} ? $_GLOBALS{insecure}
                    :  0,
        CACERT      => exists $opts->{cacert} ? 1
                    :  exists $_GLOBALS{cacert} ? $_GLOBALS{cacert}
                    :  undef,
        CAPATH      => exists $opts->{capath} ? 1
                    :  exists $_GLOBALS{capath} ? $_GLOBALS{capath}
                    :  undef,
        CERT        => exists $opts->{cert} ? 1
                    :  exists $_GLOBALS{cert} ? $_GLOBALS{cert}
                    :  undef,
        KEY         => exists $opts->{key} ? 1
                    :  exists $_GLOBALS{key} ? $_GLOBALS{key}
                    :  undef,
    );
    CLI::Helpers::override(verbose => 1) if $DEF{NOOP};

    if( $DEF{NOPROXY} ) {
        debug("Removing any active HTTP Proxies from ENV.");
        delete $ENV{$_} for qw(http_proxy HTTP_PROXY);
    }


    # Build the Index Pattern
    $PATTERN = $DEF{PATTERN};

    my @ordered = qw(* DATE ANY);
    foreach my $literal ( @ordered ) {
        $PATTERN =~ s/\Q$literal\E/$PATTERN_REGEX{$literal}/g;
    }

}

# Regexes for Pattern Expansion
our $CURRENT_VERSION;
my  $CLUSTER_MASTER;

=config es_globals($key)

Grab the value of the global value from the es-utils.yaml files.

=cut

sub es_globals {
    my ($key) = @_;

    es_utils_initialize() unless keys %DEF;

    return unless exists $_GLOBALS{$key};
    return $_GLOBALS{$key};
}

=head1 AUTHENTICATION

HTTP Basic Authorization is only supported when the C<proto> is set to B<https>
as not to leak credentials all over.

The username is selected by going through these mechanisms until one is found:

    --http-username
    'http-username' in /etc/es-utils.yml or ~/.es-utils.yml
    Netrc element matching the hostname of the request
    CLI::Helpers prompt()

Once the username has been resolved, the following mechanisms are tried in order:

    Netrc element matching the hostname of the request
    Password executable defined by --password-exec
    'password-exec' in /etc/es-utils.yml, ~/.es-utils.yml
    CLI::Helpers prompt()

=head2 Password Exec

It is B<BAD> practice to specify passwords as a command line argument, or store it in a plaintext
file.  There are cases where this may be necessary, but it is not recommended.  The best method for securing  your
password is to use the B<password-exec> option.

This option must point to an executable script.  That script will be passed two arguments, the hostname and the username
for the request.  It expects the password printed to STDOUT as the last line of output.  Here's an example password-exec setup
using Apple Keychain:

    #!/bin/sh

    HOSTNAME=$1;
    USERNAME=$2;

    /usr/bin/security find-generic-password -w -a "$USERNAME" -s "$HOSTNAME"

If we save this to "$HOME/bin/get-passwd.sh" we can execute a script
like this:

    $ es-search.pl --http-username bob --password-exec $HOME/bin/get-passwd.sh \
                    --base secure-data --fields

Though it's probably best to set this in your ~/.es-utils.yml file:

    ---
    host: secured-cluster.example.org
    port: 443
    proto: https
    http-username: bob
    password-exec: /home/bob/bin/get-passwd.sh

=head3 CLI::Helpers and Password Prompting

If all the fails to yield a password, the last resort is to use CLI::Helpers::prompt() to ask the user for their
password.  If the user is using version 1.1 or higher of CLI::Helpers, this call will turn off echo and readline magic
for the password prompt.

=config es_basic_auth($host)

Get the user/password combination for this host.  This is called from LWP::UserAgent if
it recieves a 401, so the auth condition must be satisfied.

Returns the username and password as a list.

=cut

my %_auth_cache = ();

sub es_basic_auth {
    my ($host) = @_;

    es_utils_initialize() unless keys %DEF;

    $host ||= $DEF{HOST};

    # Return the results if we've done this already
    return @{ $_auth_cache{$host} }{qw(username password)}
        if exists $_auth_cache{$host};

    # Set the cached element
    my %auth = ();

    # Lookup the details netrc
    my $netrc = Net::Netrc->lookup($host);
    if( $DEF{HOST} eq $host ) {
        %auth = map { lc($_) => $DEF{$_} } qw(USERNAME);
    }
    my %meta = ();
    foreach my $k (qw( http-username password-exec )) {
        foreach my $name ( $DEF{INDEX}, $DEF{BASE} ) {
            next unless $name;
            if( my $v = es_local_index_meta($k, $name) ) {
                $meta{$k} = $v;
                last;
            }
        }
    }

    # Get the Username
    $auth{username} ||= $meta{'http-username'} ? $meta{'http-username'}
                      : defined $DEF{USERNAME} ? $DEF{USERNAME}
                      : defined $netrc         ? $netrc->login
                      : $ENV{USER};

    # Prompt for the password
    $auth{password} ||= defined $netrc ? $netrc->password
                      : (es_pass_exec($host,$auth{username},$meta{'password-exec'})
                            || prompt(sprintf "Password for '%s' at '%s': ", $auth{username}, $host)
                        );

    # Store
    $_auth_cache{$host} = \%auth;
    return @auth{qw(username password)};
}

=func es_pass_exec(host, username)

Called from es_basic_auth to exec a program, capture the password
and return it to the caller.  This allows the use of password vaults
and keychains.

=cut

sub es_pass_exec {
    my ($host,$username,$exec) = @_;

    es_utils_initialize() unless keys %DEF;

    # Simplest case we can't run
    $exec ||= $DEF{PASSEXEC};
    return unless length $exec && -x $exec;

    my(@out,@err);
    # Run the password command captue out, error and RC
    run3 [ $exec, $host, $username ], \undef, \@out, \@err;
    my $rc = $?;

    # Record the error
    if( @err or $rc != 0 ) {
        output({color=>'red',stderr=>1},
            sprintf("es_pass_exec() called '%s' and met with an error code '%d'", $exec, $rc),
            @err
        );
        return;
    }

    # Format and return the result
    my $passwd = $out[-1];
    chomp($passwd);
    return $passwd;
}


=func es_pattern

Returns a hashref of the pattern filter used to get the indexes
    {
        string => '*',
        re     => '.*',
    }

=cut

sub es_pattern {
    es_utils_initialize() unless keys %DEF;
    return {
        re     => $PATTERN,
        string => $DEF{PATTERN},
    };
}

sub _get_ssl_opts {
    es_utils_initialize() unless keys %DEF;
    my %opts = ();
    $opts{SSL_ca_file}     = $DEF{CACERT} if $DEF{CACERT};
    $opts{SSL_ca_path}     = $DEF{CAPATH} if $DEF{CAPATH};
    $opts{SSL_cert_file}   = $DEF{CERT}   if $DEF{CERT};
    $opts{SSL_key_file}    = $DEF{KEY}    if $DEF{KEY};

    # Disable Certificate Verification
    if ( $DEF{INSECURE} ) {
        $opts{verify_hostname} = 0;
        $opts{SSL_verify_mode} = 0x00;
    }
    return \%opts;
}

sub _get_es_version {
    return $CURRENT_VERSION if defined $CURRENT_VERSION;
    my $conn = es_connect();
    # Build the request
    my $req  = App::ElasticSearch::Utilities::HTTPRequest->new(
        GET => sprintf "%s://%s:%d",
                    $conn->proto, $conn->host, $conn->port
    );
    # Check if we're doing auth
    my @auth = $DEF{PASSEXEC} ? es_basic_auth($conn->host) : ();
    # Add authentication if we get a password
    $req->authorization_basic( @auth ) if @auth;

    # Retry with TLS and/or Auth
    my %try = map { $_ => 1 } qw( tls auth );
    my $resp;
    while( not defined $CURRENT_VERSION ) {
        $resp = $conn->ua->request($req);
        if( $resp->is_success ) {
            my $ver;
            eval {
                $ver = $resp->content->{version};
            };
            if( $ver ) {
                if( $ver->{distribution} and $ver->{distribution} eq 'opensearch' ) {
                    $CURRENT_VERSION = version->parse($ver->{minimum_wire_compatibility_version});
                }
                else {
                    $CURRENT_VERSION = version->parse($ver->{number});
                }
            }
        }
        elsif( $resp->code == 500 && $resp->message eq "Server closed connection without sending any data back" ) {
            # Try TLS
            last unless $try{tls};
            delete $try{tls};
            $conn->proto('https');
            warn "Attempting promotion to HTTPS, try setting 'proto: https' in ~/.es-utils.yaml";
        }
        elsif( $resp->code == 401 ) {
            # Retry with credentials
            last unless $try{auth};
            delete $try{auth};
            warn "Authorization required, try setting 'password-exec: /home/user/bin/get-password.sh` in ~/.es-utils.yaml'"
                unless $DEF{PASSEXEC};
            $req->authorization_basic( es_basic_auth($conn->host) );
        }
        else {
            warn "Failed getting version";
            last;
        }
    }
    if( !defined $CURRENT_VERSION || $CURRENT_VERSION <= 2 ) {
        output({color=>'red',stderr=>1}, sprintf "[%d] Unable to determine Elasticsearch version, something has gone terribly wrong: aborting.", $resp->code);
        output({color=>'red',stderr=>1}, ref $resp->content ? YAML::XS::Dump($resp->content) : $resp->content) if $resp->content;
        exit 1;
    }
    debug({color=>'magenta'}, "FOUND VERISON '$CURRENT_VERSION'");
    return $CURRENT_VERSION;
}

=conn es_connect

Without options, this connects to the server defined in the args.  If passed
an array ref, it will use that as the connection definition.

=cut

my $ES = undef;

sub es_connect {
    my ($override_servers) = @_;

    es_utils_initialize() unless keys %DEF;

    my %conn = (
        host     => $DEF{HOST},
        port     => $DEF{PORT},
        proto    => $DEF{PROTO},
        timeout  => $DEF{TIMEOUT},
        ssl_opts => _get_ssl_opts,
    );
    # Only authenticate over TLS
    if( $DEF{PROTO} eq 'https' ) {
        $conn{username} = $DEF{USERNAME};
        $conn{password} = es_pass_exec(@DEF{qw(HOST USERNAME)}) if $DEF{PASSEXEC};
    }

    # If we're overriding, return a unique handle
    if(defined $override_servers) {
        my @overrides = is_arrayref($override_servers) ? @$override_servers : $override_servers;
        my @servers;
        foreach my $entry ( @overrides ) {
            my ($s,$p) = split /\:/, $entry;
            $p ||= $conn{port};
            push @servers, { %conn, host => $s, port => $p };
        }

        if( @servers > 0 ) {
            my $pick = @servers > 1 ? $servers[int(rand(@servers))] : $servers[0];
            return App::ElasticSearch::Utilities::Connection->new(%{$pick});
        }
    }
    else {
        # Check for index metadata
        foreach my $k ( keys %conn ) {
            foreach my $name ( $DEF{INDEX}, $DEF{BASE} ) {
                next unless $name;
                if( my $v = es_local_index_meta($k => $name) ) {
                    $conn{$k} = $v;
                    last;
                }
            }
        }
    }

    # Otherwise, cache our handle
    $ES ||= App::ElasticSearch::Utilities::Connection->new(%conn);

    return $ES;
}

=func es_master([$handle])

Returns true (1) if the handle is to the the cluster master, or false (0) otherwise.

=cut

sub es_master {
    my ($instance) = @_;
    if(!defined $instance && defined $CLUSTER_MASTER) {
        return $CLUSTER_MASTER;
    }
    my $is_master = 0;
    my @request = ('/_cluster/state/master_node');
    unshift @request, $instance if defined $instance;

    my $cluster = es_request(@request);
    if( defined $cluster && $cluster->{master_node} ) {
        my $local = es_request('/_nodes/_local');
        if ($local->{nodes} && $local->{nodes}{$cluster->{master_node}}) {
            $is_master = 1;
        }
    }
    $CLUSTER_MASTER = $is_master unless defined $instance;
    return $is_master;
}

=conn es_request([$handle],$command,{ method => 'GET', uri_param => { a => 1 } }, {})

Retrieve URL from ElasticSearch, returns a hash reference

First hash ref contains options, including:

    uri_param           Query String Parameters
    index               Index name
    type                Index type
    method              Default is GET

If the request is not successful, this function will throw a fatal exception.
If you'd like to proceed you need to catch that error.

=cut

sub es_request {
    my $instance = ref $_[0] eq 'App::ElasticSearch::Utilities::Connection' ? shift @_ : es_connect();

    $CURRENT_VERSION = _get_es_version() if !defined $CURRENT_VERSION;

    my($url,$options,$body) = _fix_version_request(@_);

    # Normalize the options
    $options->{method} ||= 'GET';
    $options->{command} = $url;
    my $index;

    if( exists $options->{index} ) {
        if( my $index_in = delete $options->{index} ) {
            # No need to validate _all
            if( $index_in eq '_all') {
                $index = $index_in;
            }
            else {
                # Validate each included index
                my @indexes = is_arrayref($index_in) ? @{ $index_in } : split /\,/, $index_in;
                $index = join(',', @indexes);
            }
        }
    }

    # For the cat api, index goes *after* the command
    if( $url =~ /^_(cat|stats)/ && $index ) {
        $url =~ s/\/$//;
        $url = join('/', $url, $index);
        delete $options->{command};
    }
    elsif( $index ) {
        $options->{index} = $index;
    }
    else {
        $index = '';
    }

    # Figure out if we're modifying things
    my $modification = $url eq '_search' && $options->{method} eq 'POST' ? 0
                     : $options->{method} ne 'GET';

    if($modification) {
        # Set NOOP if necessary
        if(!$DEF{NOOP} && $DEF{MASTERONLY}) {
            if( !es_master() ) {
                $DEF{NOOP} = 1;
            }
        }

        # Check for noop
        if( $DEF{NOOP} ) {
            my $flag = $DEF{MASTERONLY} && !es_master() ? '--master-only' : '--noop';
            output({color=>'cyan'}, "Called es_request($index/$options->{command}), but $flag set and method is $options->{method}");
            return;
        }
    }

    # Make the request
    my $resp = $instance->request($url,$options,$body);

    # Check the response is defined, bail if it's not
    die "Unsupported request method: $options->{method}" unless defined $resp;

    # Logging
    verbose({color=>'yellow'}, sprintf "es_request(%s/%s) returned HTTP Status %s",
        $index, $options->{command}, $resp->message,
    ) if $resp->code != 200;

    # Error handling
    if( !$resp->is_success ) {
        my $msg;
        eval {
            my @causes = ();
            foreach my $cause ( @{ $resp->content->{error}{root_cause} } ) {
                push @causes, $cause->{index} ? "$cause->{index}: $cause->{reason}" : $cause->{reason};
            }
            $msg = join("\n", map { "\t$_" } @causes);
            1;
        } or do {
            # Default to the message, though it's usually unhelpful
            $msg = $resp->{message};
        };
        die sprintf "es_request(%s/%s) failed[%d]:\n%s",
                    $index, $options->{command}, $resp->code, $msg || 'missing error message';

    } elsif( !defined $resp->content || ( !is_ref($resp->content) && !length $resp->content )) {
        output({color=>'yellow',stderr=>1},
            sprintf "es_request(%s/%s) empty response[%d]: %s",
                $index, $options->{command}, $resp->code, $resp->message
        );
    }

    return $resp->content;
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
        foreach my $id ( keys %{ $res->{nodes} } ) {
            $_nodes{$id} = $res->{nodes}{$id}{name};
        }
    }

    return wantarray ? %_nodes : { %_nodes };
}

=indices es_indices_meta

Returns the hash of index meta data.

=cut

my $_indices_meta;
sub es_indices_meta {
    if(!defined $_indices_meta) {
        my $result = es_request('_cluster/state/metadata');
        if ( !defined $result ) {
            output({stderr=>1,color=>"red"}, "es_indices_meta(): Unable to locate indices in status!");
            exit 1;
        }
        $_indices_meta = $result->{metadata}{indices};
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

    es_utils_initialize() unless keys %DEF;

    # Seriously, English? Do you speak it motherfucker?
    $args{state} = 'close' if $args{state} eq 'closed';

    my @indices = ();
    my %idx = ();
    my $wildcard = !exists $args{_all} && defined $DEF{BASE} ? sprintf "/*%s*", $DEF{BASE} : '';

    # Simplest case, single index
    if( defined $DEF{INDEX} ) {
        push @indices, $DEF{INDEX};
    }
    # Next simplest case, open indexes
    elsif( !exists $args{_all} && $args{check_state} && $args{state} eq 'open' ) {
        # Use _stats because it's break neck fast
        if( my $res = es_request($wildcard . '/_stats/docs') ) {
            foreach my $idx ( keys %{ $res->{indices} } ) {
                $idx{$idx} = 'open';
            }
        }
    }
    else {
        my $res = es_request('_cat/indices' . $wildcard, { uri_param => { h => 'index,status' } });
        foreach my $entry (@{ $res }) {
            my ($index,$status) = is_hashref($entry) ? @{ $entry }{qw(index status)} : split /\s+/, $entry;
            $idx{$index} = $status;
        }
    }

    foreach my $index (sort keys %idx) {
        if(!exists $args{_all}) {
            my $status = $idx{$index};
            # State Check Disqualification
            if($args{state} ne 'all' && $args{check_state})  {
                my $result = $status eq $args{state};
                next unless $result;
            }

            my $p = es_pattern();
            next unless $index =~ /$p->{re}/;
            debug({indent=>2},"= name checks succeeded");

            if ($args{older} && defined $DEF{DAYS}) {
                my $days_old = es_index_days_old( $index );
                if (!defined $days_old || $days_old < $DEF{DAYS}) {
                    next;
                }
            }
            elsif( $args{check_dates} && defined $DEF{DAYS} ) {
                my $days_old = es_index_days_old( $index );
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

    # We retrieved these from the cluster, so preserve them here.
    $_valid_index{$_} = 1 for @indices;

    return wantarray ? @indices : \@indices;
}

=index es_index_strip_date( 'index-name' )

Returns the index name with the date removed.

=cut

sub es_index_strip_date {
    my ($index) = @_;

    return -1 unless defined $index;

    es_utils_initialize() unless keys %DEF;

    # Try the Date Pattern
    if( my $base = $index =~ s/[^a-z0-9]+$PATTERN_REGEX{DATE}.*$//rio ) {
        return $base;
    }
    return;
}

=index es_index_bases( 'index-name' )

Returns an array of the possible index base names for this index

=cut

my %_stripped=();

sub es_index_bases {
    my ($index) = @_;

    return unless defined $index;

    # Strip to the base
    my $stripped = es_index_strip_date($index);
    # Remove the rollover portion
    $stripped =~ s/[\-_.]\d+$//;
    return unless defined $stripped and length $stripped;

    # Compute if we haven't already memoized
    if( !exists $_stripped{$stripped} ) {
        my %bases=();
        my @parts = grep { defined && length } split /[-_]/, $stripped;
        debug(sprintf "es_index_bases(%s) dissected to %s", $index, join(',', @parts));
        my $sep = index( $stripped, '_' ) >= 0 ? '_' : '-';

        my %collected = ();
        foreach my $end ( 0..$#parts ) {
            my $name = join($sep, @parts[0..$end]);
            $collected{$name} = 1;
        }
        $_stripped{$stripped} = [ sort keys %collected ]
    }

    return @{ $_stripped{$stripped} };
}

=index es_index_days_old( 'index-name' )

Return the number of days old this index is.

=cut

sub es_index_days_old {
    my ($index) = @_;

    return unless defined $index;

    es_utils_initialize() unless keys %DEF;


    if( $index =~ /[^a-z0-9]$PATTERN_REGEX{DATE}/io ) {
        # Build Date Array
        my @date = map { int }
                    grep { length }
                    map { $+{$_} =~ s/^0//r } qw(day month year);
        $date[1]--; # move 1-12 -> 0-11
        # Validate
        if( @date != 3 ) {
            warn sprintf "es_index_days_old(%s) matched DATE(%s), but did not receive enough parts: %s",
                $index,
                $+{datestr},
                join(', ', map { "'$_'" } @date);
            return;
        }

        # Calculate Difference
        my $now = timegm(0,0,0,(gmtime)[3,4,5]);
        my $idx_time = eval { timegm( 0,0,0, @date ) };
        return unless $idx_time;
        my $diff = $now - $idx_time;
        $diff++;    # Add one second
        debug({color=>"yellow"}, sprintf "es_index_days_old(%s) - Time difference is %0.3f", $index, $diff/86400);
        return int($diff / 86400);
    }
    verbose({color=>"red"}, "es_index_days_old($index) - date string not found");
    return;
}


=index es_index_shards( 'index-name' )

Returns the number of replicas for a given index.

=cut

sub es_index_shards {
    my ($index) = @_;

    my %shards = map { $_ => 0 } qw(primaries replicas);
    my $result = es_request('_settings', {index=>$index});
    if( defined $result && is_hashref($result) )   {
        $shards{primaries} = $result->{$index}{settings}{index}{number_of_shards};
        $shards{replicas}  = $result->{$index}{settings}{index}{number_of_replicas};
    }

    return wantarray ? %shards : \%shards;
}

=index es_index_valid( 'index-name' )

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

=index es_index_fields('index-name')

Returns a hash reference with the following data:

    key_name:
      type: field_data_type
      # If the field is nested
      nested_path: nested_path
      nested_key: nested_key

=cut

sub es_index_fields {
    my ($index) = @_;

    my $result = es_request('_mapping', { index => $index });

    return unless defined $result;

    my %fields;
    foreach my $idx ( sort keys %{ $result } ) {
        # Handle Version incompatibilities
        my $ref = exists $result->{$idx}{mappings} ? $result->{$idx}{mappings} : $result->{$idx};

        # Loop through the mappings, skipping _default_, except on 7.x where we notice "properties"
        my @mappings = exists $ref->{properties} ? ($ref)
                     : map { $ref->{$_} } grep { $_ ne '_default_' } keys %{ $ref };
        foreach my $mapping (@mappings) {
            _find_fields(\%fields,$mapping);
        }
    }
    # Return the results
    return \%fields;
}

{
    # Closure for field metadata
    my $nested_path;

    sub _add_fields {
        my ($f,$type,@path) = @_;

        return unless @path;

        my %i = (
            type   => $type,
        );

        # Store the full path
        my $key = join('.', @path);

        if( $nested_path ) {
            $i{nested_path} = $nested_path;
            $i{nested_key}  = substr( $key, length($nested_path)+1 );
        }

        $f->{$key} = \%i;
    }

    sub _find_fields {
        my ($f,$ref,@path) = @_;

        return unless is_hashref($ref);
        # Handle things with properties
        if( exists $ref->{properties} && is_hashref($ref->{properties}) ) {
            $nested_path = join('.', @path) if $ref->{type} and $ref->{type} eq 'nested';
            foreach my $k (sort keys %{ $ref->{properties} }) {
                _find_fields($f,$ref->{properties}{$k},@path,$k);
            }
            undef($nested_path);
        }
        # Handle elements that contain data
        elsif( exists $ref->{type} ) {
            _add_fields($f,$ref->{type},@path);
            # Handle multifields
            if( exists $ref->{fields} && is_hashref($ref->{fields}) ) {
                foreach my $k (sort keys %{ $ref->{fields} } ) {
                    _add_fields($f,$ref->{type},@path,$k);
                }
            }
        }
        # Unknown data, throw an error if we care that deeply.
        else {
            debug({stderr=>1,color=>'red'},
                sprintf "_find_fields(): Invalid property at: %s ref info: %s",
                    join('.', @path),
                    join(',', is_hashref($ref) ? sort keys %{$ref} :
                            ref $ref         ? ref $ref : 'unknown ref'
                    ),
            );
        }
    }
}

=maint es_close_index('index-name')

Closes an index

=cut

sub es_close_index {
    my($index) = @_;

    return es_request('_close',{ method => 'POST', index => $index });
}

=maint es_open_index('index-name')

Open an index

=cut

sub es_open_index {
    my($index) = @_;

    return es_request('_open',{ method => 'POST', index => $index });
}

=maint es_delete_index('index-name')

Deletes an index

=cut

sub es_delete_index {
    my($index) = @_;

    return es_request('',{ method => 'DELETE', index => $index });
}

=maint es_optimize_index('index-name')

Optimize an index to a single segment per shard

=cut

sub es_optimize_index {
    my($index) = @_;

    return es_request('_forcemerge',{
            method    => 'POST',
            index     => $index,
            uri_param => {
                max_num_segments => 1,
            },
    });
}

=maint es_apply_index_settings('index-name', { settings })

Apply a HASH of settings to an index.

=cut

sub es_apply_index_settings {
    my($index,$settings) = @_;

    if(!is_hashref($settings)) {
        output({stderr=>1,color=>'red'}, 'usage is es_apply_index_settings($index,$settings_hashref)');
        return;
    }

    return es_request('_settings',{ method => 'PUT', index => $index },$settings);
}

=index es_index_segments( 'index-name' )

Exposes GET /$index/_segments

Returns the segment data from the index in hashref:

=cut

sub es_index_segments {
    my ($index) = @_;

    if( !defined $index || !length $index || !es_index_valid($index) ) {
        output({stderr=>1,color=>'red'}, "es_index_segments('$index'): invalid index");
        return;
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


=index es_index_stats( 'index-name' )

Exposes GET /$index/_stats

Returns a hashref

=cut

sub es_index_stats {
    my ($index) = @_;

    return es_request('_stats', {
        index     => $index
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

    return es_request(join('/',@cmd));
}

=func es_flatten_hash

Performs flattening that's compatible with Elasticsearch's flattening.

=cut

sub es_flatten_hash {
    my $hash = shift;
    my $_flat = flatten($hash, { HashDelimiter=>':', ArrayDelimiter=>':' });
    my %compat = map { s/:/./gr => $_flat->{$_} } keys %{ $_flat };
    return \%compat;
}

=human es_human_count

Takes a number and returns the number as a string in docs, thousands, millions, or billions.

    1_000     -> "1.00 thousand",
    1_000_000 -> "1.00 million",

=cut

sub es_human_count {
    my ($size) = @_;

    my $unit = 'docs';
    my @units = qw(thousand million billion);

    while( $size > 1000 && @units ) {
        $size /= 1000;
        $unit = shift @units;
    }

    return sprintf "%0.2f %s", $size, $unit;
}

=human es_human_size

Takes a number and returns the number as a string in bytes, Kb, Mb, Gb, or Tb using base 1024.

    1024        -> '1.00 Kb',
    1048576     -> '1.00 Mb',
    1073741824  -> '1.00 Gb',

=cut

sub es_human_size {
    my ($size) = @_;

    my $unit = 'b';
    my @units = qw(Kb Mb Gb Tb);

    while( $size > 1024 && @units ) {
        $size /= 1024;
        $unit = shift @units;
    }

    return sprintf "%0.2f %s", $size, $unit;
}

=human es_format_numeric

Takes a value and the minimum digits of significance.

=cut

sub es_format_numeric {
    my($v,$len) = @_;
    $len ||= 3;

    # If this looks like a number, format it
    if ( $v =~ /^([0-9]+)\.(0*)[0-9]*$/ ) {
        my $ints   = length($1);
        my $zeroes = length($2);
        my $precision = $len + $zeroes - $ints;
        $v = $ints > $len    ? int($v)
            : $precision > 0 ? sprintf("%0.${precision}f",$v)
            : $v;
    }

    return $v;
}

=func def('key')

Exposes Definitions grabbed by options parsing

=cut

sub def {
    my($key)= map { uc }@_;

    es_utils_initialize() unless keys %DEF;

    return exists $DEF{$key} ? $DEF{$key} : undef;
}

=func es_local_index_meta(key => 'base' || 'index')

Fetch meta-data from the local config file, i.e. C<~/.es-utils.yaml>.

Format is:

    ---
    meta:
      index_name:
        key: value
      index_basename:
        key: value

The most specific version is searched first, followed by the index stripped of
it's date, and then on through all the bases discovered with
C<es_index_bases()>.


This is used by the C<es-search.pl> utility to do lookups of the B<timestamp>
field it needs to sort documents, i.e.:

    ---
    meta:
      logstash:
        timestamp: '@timestamp'
        host: es-cluster-01.int.example.com
      bro:
        timestamp: 'timestamp'

=cut

sub es_local_index_meta {
    my ($key,$name_or_base) = @_;

    es_utils_initialize() unless keys %DEF;

    if( exists $_GLOBALS{meta} ) {
        my $meta   = $_GLOBALS{meta};
        my @search = ( $name_or_base );
        push @search, es_index_strip_date($name_or_base);
        push @search, es_index_bases($name_or_base);

        foreach my $check ( @search ) {
            if( exists $meta->{$check} && exists $meta->{$check}{$key} ) {
                return $meta->{$check}{$key};
            }
        }
    }

    return;
}

=head1 SYNOPSIS

This distribution contains modules for interacting with ElasticSearch and
OpenSearch and utility scripts.

=head1 SCRIPTS

This a set of utilities to make monitoring ElasticSearch clusters much simpler.

=head2 SEARCHING

    scripts/es-aggregate.pl - Utility to search and aggregate index contents
    scripts/es-search.pl - Utility to search and explore index contents

=head2 MONITORING

    scripts/es-graphite-dynamic.pl - Perform index maintenance on daily indexes
    scripts/es-index-fields.pl - Collect and report on field statistics for indices
    scripts/es-index-scan.pl - Scan for potential index issues
    scripts/es-nodes.pl - View node information
    scripts/es-status.pl - Command line utility for ES Metrics
    scripts/es-storage-overview.pl - View how shards/data is aligned on your cluster

=head2 MAINTENANCE

    scripts/es-alias-manager.pl - Manage index aliases automatically
    scripts/es-daily-index-maintenance.pl - Perform index maintenance on daily indexes
    scripts/es-index-blocks.pl - Report and fix any blocks on indices
    scripts/es-open.pl - Open any closed indices matching a index parameters

=head2 MANAGEMENT

    scripts/es-apply-settings.pl - Apply settings to all indexes matching a pattern
    scripts/es-cluster-settings.pl - Manage cluster settings
    scripts/es-copy-index.pl - Copy an index from one cluster to another
    scripts/es-storage-overview.pl - View how shards/data is aligned on your cluster

The App::ElasticSearch::Utilities module simply serves as a wrapper around the scripts for packaging and
distribution.

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
