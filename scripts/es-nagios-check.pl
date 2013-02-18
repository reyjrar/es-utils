#!/usr/bin/env perl
# PODNAME: es-nagios-check.pl
# ABSTRACT: ElasticSearch Nagios Checks
use strict;

use LWP::Simple;
use JSON;
use Pod::Usage;
use Getopt::Long;

BEGIN {
    delete $ENV{$_} for qw{http_proxy};
}

#------------------------------------------------------------------------#
# Option Parsing
my %OPT=();
GetOptions(\%OPT,
    'host|H:s',
    'nodes|n:i',
    'index|i:s',
    'shard-state',
    'max-segments:i',
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $OPT{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $OPT{manual};

#------------------------------------------------------------------------#
# Globals
my %STATUS = (
    SUCCESS  => 0,
    WARNING  => 1,
    CRITICAL => 2,
    UNKNOWN  => 3,
);
my $j = JSON->new();
my %stats = ();
my %RESULTS = ();
my $RC = 0;
# Host defaults to localhost
$OPT{host} ||= 'localhost';

my @CHECKS = (
    { name => 'health',    url => qq{http://$OPT{host}:9200/_cluster/health},     require => [qw(host)] },
    { name => 'index',     url => qq{http://$OPT{host}:9200/$OPT{index}/_status}, require => [qw(host index)] },
    { name => 'segments',  url => qq{http://$OPT{host}:9200/_segments},           require => [qw(host index)] },
);

#------------------------------------------------------------------------#
# Poll ElasticSearch for Information
my $checks_performed = 0;
foreach my $check ( @CHECKS ) {
    my $run_check = 1;
    foreach my $field ( @{ $check->{require} } ){
        unless( exists $OPT{$field} && defined $OPT{$field} && length $OPT{$field} ) {
            $run_check = 0;
        }
    }

    # Skip if we don't have enough information
    next unless $run_check;

    $checks_performed++;

    # Grab the data
    eval {
        my $json = get( $check->{url} );
        die "no content returned" unless defined $json;
        $stats{$check->{name}} = $j->decode( $json );
    };
    if( my $err = $@ ) {
        nagios_status( $STATUS{CRITICAL}, CONNECT => "ERROR in $check->{name} ($check->{url}): $err" );
    }
    else {
        nagios_status( $STATUS{SUCCESS}, "FETCH_" . uc($check->{name}) => "fetch for $check->{name}" );
    }
}

if( $checks_performed == 0 ) {
    nagios_status( $STATUS{UNKNOWN}, FETCH => "No URLs were polled, something weird happened" );
    nagios_exit_properly();
}


#------------------------------------------------------------------------#
# Basic Health Checks
if( !exists $stats{health} || !defined $stats{health} ) {
    nagios_exit_properly();
}
if( $stats{health}->{status} eq 'red' ) {
    nagios_status( $STATUS{CRITICAL}, HEALTH => "cluster status RED");
}
elsif( $stats{health}->{status} eq 'yellow' ) {
    nagios_status( $STATUS{WARNING},  HEALTH => "cluster status yellow");
}
else {
    if( $stats{health}->{unassigned_shards} > 0 ) {
        nagios_status( $STATUS{WARNING},  HEALTH => "unassigned shards: ". $stats{health}->{unassigned_shards});
    }
    else {
        nagios_status( $STATUS{SUCCESS},  HEALTH => "cluster health green" );
    }
}
# Node Check
if( exists $OPT{nodes} && $OPT{nodes} > 0 ) {
    my $status = "SUCCESS";
    if( $stats{health}->{number_of_nodes} < $OPT{nodes} ) {
        $status = $stats{health}->{number_of_nodes} > ( $OPT{nodes} / 2 ) ? "WARNING" : "CRITICAL";
    }
    nagios_status( $STATUS{$status}, NODES => "found $stats{health}->{number_of_nodes} of $OPT{nodes} active" );
}
# Unassigned or Initializing Shards
if( exists $OPT{'shard-state'} ) {
    my @status=();
    my $status = 'SUCCESS';
    if( $stats{health}->{relocating_shards} > 0 ) {
        $status = 'WARNING';
        push @status, "$stats{health}->{relocating_shards} relocating";
    }
    if( $stats{health}->{initializing_shards} > 0 ) {
        $status = 'WARNING';
        push @status, "$stats{health}->{initializing_shards} initializing";
    }
    if( $stats{health}->{unassigned_shards} > 0 ) {
        $status = "CRITICAL";
        push @status, "$stats{health}->{unassigned_shards} unassigned";
    }
    push @status, "normal" unless scalar @status;

    nagios_status( $STATUS{$status}, SHARDS => "shard status: " . join(', ', @status));
}

#------------------------------------------------------------------------#
# Index Status Checking
if( exists $stats{index} && defined $stats{index} ) {
    # Get the actual index name in case this is an alias
    my $index = (keys %{ $stats{index}->{indices} })[0];

    # Index Status Check
    if( $stats{index}->{ok} ) {
        nagios_status( $STATUS{SUCCESS}, INDEX => "index $OPT{index}");
    }
    else {
        nagios_status( $STATUS{CRITICAL}, INDEX => "index $OPT{index} not ok");
    }

    # Check max segments?
    if( $OPT{'max-segments'} > 0 ) {
        my $MAX = $OPT{'max-segments'};

        my %over = ();
        foreach my $id ( keys %{ $stats{segments}->{indices}{$index}{shards} } ) {
            my $shard = $stats{segments}->{indices}{$index}{shards}{$id}[0];
            if( $shard->{num_search_segments} > $MAX ) {
                $over{$id} = $shard->{num_search_segments};
            }
        }
        if( scalar keys %over ) {
            nagios_status(
                $STATUS{WARNING},
                SEGMENTS => "Max number of segments ($MAX) exceed on shards: "
                        . join(', ', map { "$_=$over{$_}" } keys %over )
            );
        }
        else {
            nagios_status( $STATUS{SUCCESS}, SEGMENTS => "all shards have $MAX or fewer segments");
        }
    }
}
elsif( exists $OPT{index} && defined $OPT{index} or exists $OPT{'max-segments'} && defined $OPT{'max-segments'}) {
    nagios_status( $STATUS{CRITICAL}, INDEX => "All requested checks could not be completed!" );
}

#------------------------------------------------------------------------#
# Happy, Happy Joy, Joy
nagios_exit_properly();

#------------------------------------------------------------------------#
# Support Functions
#------------------------------------------------------------------------#
sub nagios_status {
    my ($rc,$name,$msg) = @_;

    # First set the status
    $RC = $rc if( $rc > $RC );

    # Store the results
    push @{ $RESULTS{$rc} }, {
        name => $name,
        msg  => $msg,
    };
    # return happiness and joy
    return 1;
}
sub nagios_exit_properly {

    # dump the status of the results
    foreach my $rc ( sort { $b <=> $a } keys %RESULTS ) {
        my $status = $rc ? "not ok" : "ok";
        my @details = ();
        foreach my $r ( @{ $RESULTS{$rc} } ) {
            push @details, $rc ? "$r->{name} - $r->{msg}" : $r->{name};
        }
        print "$status: ", join(", ", @details), "\n";
    }

    # Exit with the proper code
    exit $RC;
}

__END__

=head1 NAME

es-nagios-check.pl - Nagios Plugin to Check Elastic Search

=head1 SYNOPSIS

es-nagios-check.pl -H [host] [options]

Options:

    --help          print help
    --host|-H       Host to poll for statistics (default: localhost)
    --nodes         The number of nodes expected in the cluster, alarm on variance.
    --shard-state   Check the status of shard allocation
    --index         Index to check performance statistics (Default: NONE)
    --max-segments  Max number of segments in each shard

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<host>

Optional, the host to check, defaults to localhost

=item B<nodes>

Optional, if specified checks for this exact number of nodes in the cluster.

=item B<shard-state>

Optional, if specified checks for relocating, initializing, or unassigned shards.

=item B<index>

Optional, if specified, state and performance data for this index will be polled.

=item B<max-segments>

The more segments in a shard, the slower searches will be against that shard.
This option requires the --index flag in order to establish the correct sets of
shards to check for segmentation.

=back

=head1 DESCRIPTION

This is a plugin to poll elasticsearch for it's status and report back to Nagios.

=cut
