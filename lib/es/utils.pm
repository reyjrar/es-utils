# ABSTRACT: Utilities for Monitoring ElasticSearch
package es::utils;

use IPC::Run3;
use Term::ANSIColor;

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
sub get_stats {
    my $stat_path = shift;

    my $stats = undef;
    eval {
        my $url = "$DEF{BASE_URL}/$stat_path";
        my $json = get( $url );
        die "retreival of $url failed to return data" unless $json;
        $stats =  $JSON->decode($json);
    };
    if( my $err = $@ ){
        output({color=>"red"}, "Encountered error: $err" );
        exit 1;
    }

    return $stats;
}
sub colorize {
    my ($color,$string) = @_;

    if( $DEF{NICOLAI} ) {
        $string = nicolai_colorize( $string );
    }
    elsif( defined $color && $DEF{COLOR} ) {
        $string=colored([ $color ], $string);
    }
    return $string;
}
sub output {
    my $opts = ref $_[0] eq 'HASH' ? shift @_ : {};

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
    # Do clearing
    print "\n"x$opts->{clear} if exists $opts->{clear};
    # Print output
    print "${indent}$_\n" for @output;
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



=head1 SYNOPSIS

This a set of utilities to make monitoring ElasticSearch clusters much simpler.

Included is:

    scripts/es-status.pl - Command line utility for ES Metrics
    scripts/es-metrics-to-graphite.pl - Send ES Metrics to Graphite or Cacti
    scripts/es-nagios-check.pl - Monitor ES remotely or via NRPE with this script
    scripts/es-logstash-maintenance.pl - Perform index maintenance on logstash indexes

The es::utils module simply serves as a wrapper around the scripts for packaging and
distribution.

=head1 INSTALL

To install the utilities, simply:

    export RELEASE=0.010

    wget --no-check-certificate https://github.com/reyjrar/es-utils/blob/master/releases/es-utils-$RELEASE.tar.gz?raw=true -O es-utils.tgz

    tar -zxvf es-utils.tgz

    cd es-utils-$RELEASE

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
    es-logstash-maintenance.pl --help

For individual options and capabilities


=cut


1;
