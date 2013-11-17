# ABSTRACT: Utilities for Monitoring ElasticSearch
package App::ElasticSearch::Utilities;

# VERSION

use strict;
use warnings;

our $ES_CLASS = undef;
our $_OPTIONS_PARSED;

# Because of the poor decision to upload both ElasticSearch and Elasticsearch,
# We need to support both libraries due to some production freezes of ElasticSearch.
BEGIN {
    if( eval { require Elasticsearch::Compat; } ) {
        $ES_CLASS = "Elasticsearch::Compat";
    }
    elsif( eval { require ElasticSearch; } ) {
        $ES_CLASS = "ElasticSearch";
    }
    else {
        die "Please install Elasticsearch::Compat";
    }
}

use CLI::Helpers qw(:all);
use Getopt::Long qw(:config pass_through);
use Sub::Exporter -setup => {
    exports => [
        qw(es_class es_interface es_pattern es_indices)
    ],
};

=head1 ARGS

From App::ElasticSearch::Utilities:

    --local         Use localhost as the elasticsearch host
    --host          ElasticSearch host to connect to
    --port          HTTP port for your cluster
    --index         Index to run commands against
    --base          For daily indexes, reference only those starting with "logstash"
                     (same as --pattern logstash-* or logstash-DATE)
    --pattern       Use a pattern to operate on the indexes

=cut

my %opt = ();
if( !defined $_OPTIONS_PARSED ) {
    GetOptions(\%opt,
        'local',
        'host:s',
        'port:i',
        'index:s',
        'pattern:s',
        'base|index-basename:s',
    );
    $_OPTIONS_PARSED = 1;
}
# Set defaults
my %DEF = (
    HOST        => exists $opt{host} ? $opt{host} :
                   exists $opt{local} ? 'localhost' : 'localhost',
    PORT        => exists $opt{port} ? $opt{port} : 9200,
    INDEX       => exists $opt{index} ? $opt{index} : undef,
    BASE        => exists $opt{base} ? $opt{base} :
                   exists $opt{'index-basename'} ? $opt{'index-basename'} :
                   undef,
    PATTERN     => exists $opt{pattern} ? $opt{pattern} : '*',
);
debug_var(\%DEF);

# Regexes for Pattern Expansion
my %PATTERN_REGEX = (
    '*'  => qr/.*/,
    '?'  => qr/.?/,
    DATE => qr/\d{4}[.\-]?\d{2}[.\-]?\d{2}/,
    ANY  => qr/.*/,
);

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
    string => $DEF{pattern},
);
sub es_pattern {
    return wantarray ? %_pattern : { %_pattern };
}


=func es_class

Return the name of the Elasticsearch class implementing our functionality.

=cut

sub es_class {
    return $ES_CLASS;
}

=func es_interface

Call this to retrieve the Elasticsearch object for making calls to the cluster.

=cut

my $ES = undef;

sub es_interface {
    no strict 'refs';

    $ES ||= $ES_CLASS->new(
        servers    => [ "$DEF{HOST}:$DEF{PORT}" ],
        timeout    => 0,
        transport  => 'http',
        no_refresh => 1,
    );

    return $ES;
}

=func es_indices

Returns a list of active indexes matching the filter criteria specified on the command
line.

=cut

sub es_indices {
    my @indices = ();

    my $es = es_interface;

    return @indices;
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


=head2 CONTRIBUTORS

    Mihai Oprea <mishu@mishulica.com>
    Samit Badle

=cut


1;
