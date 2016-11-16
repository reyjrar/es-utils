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

# VERSION

use App::ElasticSearch::Utilities::HTTPRequest;
use CLI::Helpers qw(:output);
use JSON::MaybeXS;
use LWP::UserAgent;
use Module::Load;
use Ref::Util qw(is_ref is_arrayref is_hashref);
use Sub::Quote;
use URI;
use URI::QueryParam;

use Moo;
use namespace::autoclean;

=attr host

Hostname or ip to connect to, default 'B<localhost>'

=cut
has 'host' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a hostname or ip for host parameter" unless defined $_[0] and length $_[0] }),
    default => quote_sub(q{'localhost'}),
);

=attr port

Port to connect the HTTP transport for the ElasticSearch cluster, default is B<9200>

=cut
has 'port' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a port number" unless defined $_[0] and $_[0] =~ /^\d+$/ }),
    default => quote_sub(q{9200}),
);

=attr proto

Protocol to use, defaults to 'B<http>'.

This module converts from the performance concerned backend of B<Hijk> and B<Elastijk>, to the feature
rich B<LWP::UserAgent>.  This means we can now support TLS communication to the ES back-end and things like
basic authentication.

=cut

has 'proto' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a protocol either http or https" unless defined $_[0] and $_[0] =~ /^http(s)?/}),
    default => quote_sub(q{'http'}),
);

=attr timeout

Connection and Read Timeout for the HTTP connection, defaults to B<10> seconds.

=cut

has 'timeout' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a timeout in seconds" unless defined $_[0] and $_[0] =~ /^\d+$/ }),
    default => quote_sub(q{10}),
);

=attr ua

Lazy built B<LWP::UserAgent> to access LWP::UserAgent directly.

=cut

has 'ua' => (
    is  => 'lazy',
    isa => quote_sub(q{die "UA setup failed." unless ref($_[0]) =~ /^LWP::UserAgent/}),
);


# Monkey Patch LWP::UserAgent to use our credentials
{
    no warnings 'redefine';

    sub LWP::UserAgent::get_basic_credentials {
        my ($self,$realm,$url) = @_;
        my $uri = URI->new( $url );
        load App::ElasticSearch::Utilities => 'es_basic_auth';
        return es_basic_auth( $uri->host );
    }
}

sub _build_ua {
    my ($self) = @_;

    # Construct the UA Object
    my $local_version = eval '$VERSION' || '999.9';
    my $ua = LWP::UserAgent->new(
        keep_alive        => 3,
        agent             => sprintf("%s/%0.1f (Perl %s)", __PACKAGE__, $local_version, $^V),
        protocols_allowed => [qw(http https)],
    );
    debug({color=>'cyan'}, sprintf "Initialized a UA: %s", $ua->agent);

    # Decode the JSON Automatically
    $ua->add_handler( response_done => sub {
        my ($response,$lwp_ua,$headers) = @_;
        debug( {color=>'magenta'}, "respone_done handler, got:");

        debug_var($response);
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
                map { s/^\s+//; s/\s+$//; $_ }
                grep { defined && length }
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

    # Build the Path
    my @path = grep { defined && length } @{ $options }{qw(index command)};
    my $path = join('/', @path);

    debug(sprintf "calling %s->request(%s)", ref $self, $path);

    # Build a URI
    my $uri = URI->new( sprintf "%s://%s:%d",
        $self->proto,
        $self->host,
        $self->port,
    );
    $uri->path( join('/', @path) );

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
    debug({color=>'magenta'}, sprintf "Issuing %s with URI of '%s'", $method, $uri->as_string);
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
    $req->content($body) if defined $body;

    return $self->ua->simple_request( $req );
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
ElasticSearch bulk API, see: L<https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-bulk.html|Bulk API>

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
