#!/usr/bin/env perl
# PODNAME: es-copy-index.pl
# ABSTRACT: Copy an index from one cluster to another

BEGIN {
    delete $ENV{$_} for qw(http_proxy https_proxy HTTP_PROXY);
}

use strict;
use warnings;

use Carp;
use DateTime;
use ElasticSearch;
use File::Basename;
use File::Spec;
use FindBin;
use Getopt::Long;
use JSON::XS;
use MIME::Lite;
use Pod::Usage;
use Sys::Hostname;
use YAML;
use es::utils qw(:all);

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT,
    'from:s',
    'to:s',
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $OPT{manual};
my $INDEX = shift @ARGV;

# Connect to ElasticSearch
my %ES = ();
foreach my $dir (qw(from to)) {
    $ES{$dir} = ElasticSearch->new(
        servers   => "$OPT{$dir}:9200",
        transport => 'http',
        timeout   => 0,
    );
}

croak "Invalid index: $INDEX\n" unless valid_index($INDEX);
my $RECORDS = 0;
my $LAST = time;

my $scroller;
eval {
    $scroller = $ES{from}->scrolled_search(
        index => $INDEX,
        search_type => 'scan',
        scroll => '5m',
        size   => 1000,
    );
};
if( my $error = $@ ) {
    croak "ElasticSearch error: $error";
}
eval {
    $ES{to}->reindex(
        source     => $scroller,
        dest_index => $INDEX,
        transform  => \&show_counts,
        bulk_size  => 10000,
        quiet      => es::utils::def('verbose') > 0,
    );

    # Optimize
    print "Optimizing $INDEX.\n";
    $ES{to}->optimize_index(
        index            => $INDEX,
        max_num_segments => 1,
        wait_for_merge   => 0,
    );
};

sub show_counts {
    my $doc = shift;

    $RECORDS++;
    if( $RECORDS % 100_000 == 0 ) {
        my $now = time;
        my $diff = $now - $LAST;
        my @time=localtime;
        my $msg = sprintf "%02d:%02d:%02d Records: %d (diff:%0.2f)\n", @time[2,1,0], $RECORDS, $diff;
        output({color=>'yellow'}, $msg);
        $LAST=$now;
    }
    return $doc;
}


sub valid_index {
    my ($index) = @_;

    my $result;
    eval {
        $result = $ES{from}->index_exists( index => $index );
        debug("$index", Dump($result));
    };
    if( my $error = $@ ) {
        output({color=>'red',stderr=>1}, $error);
    }
    if( defined $result && exists $result->{ok} && $result->{ok} ) {
        return 1;
    }
    return 0;
}
__END__

=head1 NAME

es-copy-index.pl - Copy an index from one cluster to another

=head1 SYNOPSIS

es-copy-access.pl [index]

Options:

    --from              A server in the cluster where the index lives
    --to                A server in the cluster where the index will be copied to
    --help              print help
    --manual            print full manual
    --verbose           Send additional messages to STDERR

=head1 OPTIONS

=over 8

=item B<from>

B<REQUIRED>: hostname or IP of the source cluster

=item B<to>

B<REQUIRED>: hostname or IP of the destination cluster

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=back

=head1 DESCRIPTION

This uses the reindex API to copy data from one cluster to another

Example:

   es-copy-index.pl --from localhost --to remote.cluster.com logstash-2013.01.11

=head1 AUTHOR

Brad Lhotsky <brad.lhotsky@gmail.com>

=cut

