package App::ElasticSearch::Utilities::Connection;
# ABSTRACT: Abstract the connection element

our $VERSION = '4.7';
# VERSION

use CLI::Helpers qw(:output);
use JSON::MaybeXS;
use LWP::UserAgent;
use Ref::Util qw(is_arrayref is_hashref);
use Sub::Quote;
use URI;
use URI::QueryParam;

use Moo;
use namespace::autoclean;

has 'host' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a hostname or ip for host parameter" unless defined $_[0] and length $_[0] }),
    default => quote_sub(q{'localhost'}),
);
has 'port' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a port number" unless defined $_[0] and $_[0] =~ /^\d+$/ }),
    default => quote_sub(q{9200}),
);
has 'proto' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a protocol either http or https" unless defined $_[0] and $_[0] =~ /^http(s)?/}),
    default => quote_sub(q{'http'}),
);
has 'timeout' => (
    is      => 'ro',
    isa     => quote_sub(q{ die "must specify a timeout in seconds" unless defined $_[0] and $_[0] =~ /^\d+$/ }),
    default => quote_sub(q{10}),
);
has 'ua' => (
    is  => 'lazy',
    isa => quote_sub(q{die "UA setup failed." unless ref $_[0] eq 'LWP::UserAgent'}),
);

sub _build_ua {
    my ($self) = @_;

    # Construct the UA Object
    my $ua = LWP::UserAgent->new(
        keep_alive        => 3,
        agent             => sprintf("%s/%02.f (Perl %0.2f)", __PACKAGE__, $VERSION, $^V),
        protocols_allowed => [qw(http https)],
    );

    # Decode the JSON Automatically
    $ua->add_handler( response_done => sub {
        my ($response,$ua,$h) = @_;
        if( $response->is_success ) {
            my $ctype = $response->content_type();
            if( defined $ctype and $ctype =~ m{^application/json\b} ) {
                eval {
                    my $decoded = decode_json( $response->content );
                    $response->content($decoded);
                };
            }
        }
    });

    # Encode the JSON Automatically
    $ua->add_handler( request_prepare => sub {
        my ($request,$ua,$h) = @_;
        if( $request->content ) {
            if( is_hashref($request->content) ) {
                eval {
                    my $json = encode_json($request->content);
                    $request->content( $json );
                };
            }
            elsif( is_arrayref($request->content) ) {
                # Bulk does this
                my @body;
                foreach my $entry (@{ $request->content }) {
                    push @body, ref $entry ? encode_json($entry) : $entry;
                }
                $request->content( join("\n", @body) );
            }
        }
    });

    # Return the Constructed Object
    return $ua;
}

sub request {
    my ($self,$url,$options,$body) = @_;

    debug(sprintf "calling %s->request(%s/%s)", ref $self,
        @{ $options }{qw(index command)});

    # Build a URI
    my $uri = URI->new( sprintf "%s://%s:%d",
        $self->proto,
        $self->host,
        $self->port,
    );

    # Build the Path
    my @path = grep { defined && length } @{ $options }{qw(index command)};
    $uri->path( join('/', @path) );

    # Query String
    if( exists $options->{uri_param} and is_hashref($options->{uri_param}) ) {
        foreach my $k ( keys %{ $options->{uri_param} } ) {
            $uri->query_param( $k => $options->{uri_param}{$k} );
        }
    }

    # Determine request method
    my $method = exists $options->{method} ? uc $options->{method} : 'GET';
    debug({color=>'magenta'}, sprintf "Issuing %s with URI of '%s'", $method, $uri->as_string);

    # Make the request
    my @params = ( $uri->as_string );
    push @params, $body if defined $body;
    return $method eq 'GET'    ? $self->ua->get( @params ) :
           $method eq 'HEAD'   ? $self->ua->head( @params ) :
           $method eq 'PUT'    ? $self->ua->put( @params ) :
           $method eq 'POST'   ? $self->ua->post( @params ) :
           $method eq 'DELETE' ? $self->ua->delete( @params ) :
           undef;
}

__PACKAGE__->meta->make_immutable;
