# ABSTRACT: Utilities for Monitoring ElasticSearch
package App::ElasticSearch::Utilities;

our $VERSION = 0.999;

use strict;
use warnings;

use IPC::Run3;
use Term::ANSIColor;
use YAML;
use Getopt::Long qw(:config pass_through);
use Sub::Exporter -setup => {
    exports => [
        qw(output verbose debug debug_var)
    ],
};

# Extract the basics from the command line
my %opt = ();
GetOptions(\%opt,
    'color',
    'csv',
    'verbose+',
    'debug',
    'quiet',
);
# Set defaults
my %DEF = (
    DEBUG       => $opt{debug} || 0,
    VERBOSE     => $opt{verbose} || 0,
    COLOR       => $opt{color} || git_color_check(),
    KV_FORMART  => $opt{csv} ? ',' : ':',
    QUIET       => $opt{quiet} || 0,
);
debug_var(\%DEF);

sub def { return exists $DEF{$_[0]} ? $DEF{$_[0]} : undef }

sub git_color_check {
    my @cmd = qw(git config --global --get color.ui);
    my($out,$err);
    eval {
        run3(\@cmd, undef, \$out, \$err);
    };
    if( $@  || $err ) {
        debug("git_color_check error: $err ($@)");
        return 0;
    }
    debug("git_color_check out: $out");
    if( $out =~ /auto/ || $out =~ /true/ ) {
        return 1;
    }
    return 0;
}
sub colorize {
    my ($color,$string) = @_;

   if( defined $color && $DEF{COLOR} ) {
        $string=colored([ $color ], $string);
    }
    return $string;
}
sub output {
    my $opts = ref $_[0] eq 'HASH' ? shift @_ : {};

    # Quiet mode!
    return if $DEF{quiet};

    # Input/output Arrays
    my @input = @_;
    my @output = ();

    # Remove line endings
    chomp(@input);

    # Determine the color
    my $color = exists $opts->{color} && defined $opts->{color} ? $opts->{color} : undef;

    # Determine indentation
    my $indent = exists $opts->{indent} ? " "x(2*$opts->{indent}) : '';

    # Determine if we're doing Key Value Pairs
    my $DO_KV = (scalar(@input) % 2 == 0 ) && (exists $opts->{kv} && $opts->{kv} == 1) ? 1 : 0;

    if( $DO_KV ) {
        while( @input ) {
            my $k = shift @input;
            # We only colorize the value
            my $v = colorize($color, shift @input );
            push @output, join($DEF{KV_FORMAT}, $k, $v);
        }
    }
    else {
        foreach my $msg ( map { colorize($color, $_); } @input) {
            push @output, $msg;
        }
    }
    my $out_handle = exists $opts->{stderr} && $opts->{stderr} ? \*STDERR : \*STDOUT;
    # Do clearing
    print $out_handle "\n"x$opts->{clear} if exists $opts->{clear};
    # Print output
    print $out_handle "${indent}$_\n" for @output;
}
sub verbose {
    my $opts = ref $_[0] eq 'HASH' ? shift @_ : {};
    $opts->{level} = 1 unless exists $opts->{level};
    my @msgs=@_;

    if( !$DEF{DEBUG} ) {
        return unless $DEF{VERBOSE} >= $opts->{level};
    }
    output( $opts, @msgs );
}
sub debug {
    my $opts = ref $_[0] eq 'HASH' ? shift @_ : {};
    my @msgs=@_;
    return unless $DEF{DEBUG};
    output( $opts, @msgs );
}
sub debug_var {
    return unless $DEF{DEBUG};

    my $opts = {clear => 1};
    if( ref $_[0] eq 'HASH' && defined $_[1] && ref $_[1] ) {
        my $ref = shift;
        foreach my $k (keys %{ $ref } ) {
            $opts->{$k} = $ref->{$k};
        };
    }
    output( $opts, Dump shift);
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

The App::ElasticSearch::Utilities module simply serves as a wrapper around the scripts for packaging and
distribution.

=head1 INSTALL

To install the utilities, simply:

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

For individual options and capabilities


=cut


1;
