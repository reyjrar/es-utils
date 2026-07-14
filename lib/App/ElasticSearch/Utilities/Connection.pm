package App::ElasticSearch::Utilities::Connection;
# ABSTRACT: Abstract the connection element

=head1 SYNOPSIS

For most users, this code will never be called directly since this module
doesn't handle parameter parsing on the CLI.  To get an object, instead call:

    use App::ElasticSearch::Utilities qw(es_connect);

    my $es = es_connect();

    my $http_response_obj = $es->request('_search',
        {
            index=>'logstash',
            uri_param => {
                size => 10,
            }
        },
        {
            query => {
                query_string => "program:sshd",
            }
        }
    );

Though even this is overkill.  The B<es_request> method maintains compatability with older versions and emulates
the API you'd expect from B<Elastijk>.

=cut

use v5.16;
use warnings;

# VERSION

use App::ElasticSearch::Utilities::HTTPRequest;
use App::ElasticSearch::Utilities::VersionHacks qw(_fix_version_request);
use CLI::Helpers qw(:output);
use JSON::MaybeXS;
use LWP::UserAgent;
use Module::Load;
use Ref::Util qw(is_ref is_arrayref is_hashref);
use Types::Standard qw( Enum HashRef InstanceOf Int Str );
use URI;
use URI::QueryParam;

use Moo;
use namespace::autoclean;

=attr host

Hostname or ip to connect to, default 'B<localhost>'

=cut
has 'host' => (
    is      => 'ro',
    isa     => Str,
    default => sub { 'localhost' },
);

=attr port

Port to connect the HTTP transport for the ElasticSearch cluster, default is B<9200>

=cut
has 'port' => (
    is      => 'ro',
    isa     => Int,
    default => sub { 9200 },
);

=attr proto

Protocol to use, defaults to 'B<http>'.

This module converts from the performance concerned backend of B<Hijk> and B<Elastijk>, to the feature
rich B<LWP::UserAgent>.  This means we can now support TLS communication to the ES back-end and things like
basic authentication.

=cut

has 'proto' => (
    is      => 'rw',
    isa     => Enum[qw(http https)],
    default => sub { 'http' },
);

=attr timeout

Connection and Read Timeout for the HTTP connection, defaults to B<10> seconds.

=cut

has 'timeout' => (
    is      => 'ro',
    isa     => Int,
    default => sub { 10 },
);

=attr username

HTTP Basic Authorization username, defaults to C<$ENV{USER}>.

=cut

has 'username' => (
    is      => 'ro',
    isa     => Str,
    default => sub { $ENV{USER} },
);

=attr password

HTTP Basic Authorization password, if set, we'll try authentication.

=cut

has 'password' => (
    is => 'ro',
);

=attr ssl_opts

SSL Options for L<LWP::UserAgent/ssl_opts>.

=cut

has 'ssl_opts' => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

=attr version

Detected ElasticSearch version

=cut

has 'version' => (
    is => 'lazy',
    isa => Str,
    init_arg => undef,
);

sub _build_version {
    my $self = shift;

    # Retry with TLS and/or Auth
    my $resp = $self->request('/',{skip_version_check => 1});
    my $err;
    if( $resp->is_success ) {
        my $ver;
        eval {
            $ver = $resp->content->{version};
        };
        if( $ver ) {
            if( $ver->{distribution} and $ver->{distribution} eq 'opensearch' ) {
                return sprintf "%0.1f", version->parse($ver->{minimum_wire_compatibility_version});
            }
            else {
                return sprintf "%0.1f", version->parse($ver->{number});
            }
        } else {
            $err = "Parsing version failed";
        }
    }
    elsif( $resp->code == 500 && $resp->message eq "Server closed connection without sending any data back" ) {
        $err = "Attempting promotion to HTTPS, try setting 'proto: https' in ~/.es-utils.yaml";
    }
    elsif( $resp->code == 401 ) {
        $err = $self->password ? sprintf("Authorization failed for user '%s'", $self->username)
                               : "Authorization required, try setting 'password-exec: /home/user/bin/get-password.sh` in ~/.es-utils.yaml'";
    }
    else {
        $err = "Failed getting version";
    }
    output({color=>'red',stderr=>1}, sprintf "FAIL [%d] Unable to determine Elasticsearch version: %s", $resp->code, $err);
    output({color=>'red',stderr=>1}, ref $resp->content ? YAML::XS::Dump($resp->content) : $resp->content) if $resp->content;
    exit 1;
}

=attr ua

Lazy built B<LWP::UserAgent> to access LWP::UserAgent directly.

=cut

has 'ua' => (
    is  => 'lazy',
    isa => InstanceOf["LWP::UserAgent"],
);

sub _build_ua {
    my ($self) = @_;

    # Construct the UA Object
    ## no critic
    my $local_version = eval '$VERSION' || '999.9';
    ## use critic
    my $ua = LWP::UserAgent->new(
        keep_alive        => 3,
        agent             => sprintf("%s/%s (Perl %s)", __PACKAGE__, $local_version, $^V),
        protocols_allowed => [qw(http https)],
        timeout           => $self->timeout,
        ssl_opts          => $self->ssl_opts,
    );
    debug({color=>'cyan'}, sprintf "Initialized a UA: %s%s", $ua->agent, $self->password ? ' (password provided)' : '');

    # Decode the JSON Automatically
    $ua->add_handler( response_done => sub {
        my ($response,$lwp_ua,$headers) = @_;
        debug( {color=>'magenta'}, "respone_done handler, got:");
        debug($response->as_string);

        my $ctype = $response->content_type() || 'invalid';
        # JSON Transform
        if( $ctype =~ m{^application/json\b} ) {
            debug({color=>'yellow',indent=>1},"JSON Decoding Response Content");
            eval {
                my $decoded = decode_json( $response->content );
                $response->content($decoded);
            };
        }
        elsif ( $response->is_success && $ctype =~ m{^text/plain} ) {
            # Plain text transform for the _cat API
            debug({color=>'yellow',indent=>1},"Plain Text Transform Response Content");
            my $decoded = [
                grep { defined && length && !/^\s+$/ }
                split /\r?\n/, $response->content
            ];
            debug_var($decoded);
            $response->content($decoded);
        }
        if( my $content = $response->content ) {
            debug({color=>'yellow'}, "After translation:");
            if( is_ref($content) ) {
                debug_var( $content );
            }
            else{
                debug( $content );
            }
        }
        $_[0] = $response;
    });

    # Warn About Basic Auth without TLS
    warn "HTTP Basic Authorization configured and not using TLS, this is not supported"
        if length $self->password && $self->proto ne 'https';

    return $ua;
}

=method request( $command, { index => ... uri_param => { size => 1 } }, $body )

This method provides a wrapper between the Hijk/Elastijk request syntax and the
LWP::UserAgent flow.  It's return value is the B<HTTP::Response> object from
B<LWP::UserAgent> instead of the more simplistic return values of B<Hijk> and
B<Elastijk>.  Use B<App::ElasticSearch::Utilities::es_request> for a simpler
interface.

=cut

sub request {
    my ($self,$url,$options,$body) = @_;

    # Skip Version Check
    my $skip_version_check = delete $options->{skip_version_check};

    # Build the Path
    $options->{command} ||= $url;
    my @path = grep { defined and length } @{ $options }{qw(index command)};
    my $path = join('/', @path);
    debug(sprintf "calling %s->request(%s)", ref $self, $path);

    # Build a URI
    my $uri = URI->new( sprintf "%s://%s:%d",
        $self->proto,
        $self->host,
        $self->port,
    );
    $uri->path($path);

    # Query String
    if( exists $options->{uri_param} and is_hashref($options->{uri_param}) ) {
        foreach my $k ( keys %{ $options->{uri_param} } ) {
            $uri->query_param( $k => $options->{uri_param}{$k} );
        }
    }
    # Body Translations
    if(!defined $body && exists $options->{body}) {
        $body ||= delete $options->{body};
    }

    # Determine request method
    my $method = exists $options->{method} ? uc $options->{method} : 'GET';

    # Special Case for Index Creation
    if( $method eq 'PUT' && $options->{index} && $options->{command} eq '/' ) {
        $uri->path($options->{index});
    }

    # Apply VersionHacks
    if ( !$skip_version_check ) {
        ($url,$options,$body) = _fix_version_request($self->version,$url,$options,$body);
    }

    debug({color=>'magenta'}, sprintf "Issuing %s with URI of '%s' as '%s:%s'", $method, $uri->as_string, $self->username, length $self->password ? 'hunter2' : '');
    if( defined $body ) {
        if( is_ref($body) )  {
            debug_var({indent=>1}, $body);
        }
        else {
            debug({indent=>1}, split /\r?\n/, $body);
        }
    }

    # Make the request
    my $req = App::ElasticSearch::Utilities::HTTPRequest->new( $method => $uri->as_string );

    # Authentication
    $req->authorization_basic( $self->username, $self->password )
        if length $self->password and $self->proto eq 'https';

    $req->content($body) if defined $body;

    return $self->ua->request( $req );
}


=method exists( index => 'name' )

Takes the name of an index, returns true if the index exists, false otherwise.

=cut

sub exists {
    my ($self,%options) = @_;

    return unless exists $options{index};
    my %params = (
        method => 'HEAD',
        index  => $options{index},
    );

    return $self->request('', \%params,)->is_success;
}

=method put( body => ... , index => ... )

Parameter B<body> is required.  Puts something to an index.  This is often used to
put settings and/or mappings to an index.

Returns a list containing the HTTP Status Code, and the Response Content.

=cut

sub put {
    my ($self,%options) = @_;

    return unless exists $options{body};
    my %params = ( method => 'PUT' );
    $params{index} = $options{index} if exists $options{index};

    my $resp = $self->request('', \%params, $options{body});
    return ( $resp->code, $resp->content );
}

=method bulk( body => ..., index => ... )

Parameter B<body> is required.  The body should be an array containing the command and documents to send to the
ElasticSearch bulk API, see: L<Bulk API|https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-bulk.html>

Returns a list containing the HTTP Status Code, and the Response Content.

=cut

sub bulk {
    my ($self,%options) = @_;

    return unless exists $options{body};
    my %params = ( method => 'POST' );
    $params{index} = $options{index} if exists $options{index};

    my $resp = $self->request( '_bulk', \%params, $options{body} );
    return ( $resp->code, $resp->content );
}

__PACKAGE__->meta->make_immutable;
