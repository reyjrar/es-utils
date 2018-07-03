#!perl
# PODNAME: es-nagios-check.pl
# ABSTRACT: ElasticSearch Nagios Checks
use strict;
use warnings;

use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(es_request es_indices);
use Pod::Usage;
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);

#------------------------------------------------------------------------#
# Option Parsing
my %OPT=();
GetOptions(\%OPT,
    'nodes|n=i',
    'check-indices|c',
    'shard-state',
    'max-segments=i',
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
my %stats = ();
my %RESULTS = ();
my $RC = 0;
debug_var(\%OPT);
my @CHECKS = (
    { name => 'health',    url => q{_cluster/health}, index => 0 },
    { name => 'index',     url => q{_status},         index => 1 },
    { name => 'segments',  url => q{_segments},       index => 1 },
);

#------------------------------------------------------------------------#
# Poll ElasticSearch for Information
my $checks_performed = 0;
foreach my $check ( @CHECKS ) {
    next if $check->{index} && !$OPT{'check-indices'};
    $checks_performed++;

    # Grab the data
    my $result = es_request($check->{url},
        $check->{index} ? { index => join(',', es_indices()) } : {}
    );
    if( !defined $result ) {
        nagios_status( $STATUS{CRITICAL}, CONNECT => "ERROR in $check->{name} ($check->{url})" );
    }
    else {
        $stats{$check->{name}} = $result;
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
        # We may have unassigned shards if we're in an initialization state, and that's less severe than
        # if we have no shard initializing or relocating and there are unassigned shards.
        $status = $stats{health}->{relocating_shards} > 0 || $stats{health}->{initializing_shards} > 0 ? "WARNING": "CRITICAL";
        push @status, "$stats{health}->{unassigned_shards} unassigned";
    }
    push @status, "normal" unless scalar @status;

    nagios_status( $STATUS{$status}, SHARDS => "shard status: " . join(', ', @status));
}

#------------------------------------------------------------------------#
# Index Status Checking
if( exists $stats{index} && defined $stats{index} ) {
    # Get the actual index name in case this is an alias
    foreach my $index ( keys %{ $stats{index}->{indices} } ) {
        # Index Status Check
        if( $stats{index}->{ok} ) {
            nagios_status( $STATUS{SUCCESS}, INDEX => "$index");
        }
        else {
            nagios_status( $STATUS{CRITICAL}, INDEX => "$index not ok");
        }

        # Check max segments?
        if( exists $OPT{'max-segments'} && $OPT{'max-segments'} > 0 ) {
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

    verbose("# [$rc] $name : $msg");

    # First set the status
    $RC = $rc if( $rc > $RC );

    # Store the results
    $RESULTS{$rc} ||= {};
    $RESULTS{$rc}->{$name} ||= { count => 0, msg => [] };

    $RESULTS{$rc}->{$name}{count}++;
    push @{ $RESULTS{$rc}->{$name}{msg} }, $msg;

    # return happiness and joy
    return 1;
}
sub nagios_exit_properly {
    debug("Nagios Exited at line: " . (caller)[2]);

    # dump the status of the results
    foreach my $rc ( sort { $b <=> $a } keys %RESULTS ) {
        my $status = $rc ? "not ok" : "ok";
        my @details = ();
        foreach my $name ( sort keys %{ $RESULTS{$rc} } ) {
            my $r = $RESULTS{$rc}->{$name};
            my $msg = $name;
            $msg .= "[$r->{count}]" if $r->{count} > 1;
            $msg .= " - $r->{msg}[0]" if $rc;
            push @details, $msg;
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

    --help              print help
    --manual            Show the full manual
    --nodes             The number of nodes expected in the cluster, alarm on variance.
    --shard-state       Check the status of shard allocation
    --check-indices     Check indices for segmentation and "ok" status
    --max-segments      Max number of segments in each shard

=head1 OPTIONS

=over 8

=item B<help>

Print this message and exit

=item B<nodes>

Optional, if specified checks for this exact number of nodes in the cluster.

=item B<shard-state>

Optional, if specified checks for relocating, initializing, or unassigned shards.

=item B<check-indices>

Optional, if specified, state and performance data for the indexes selected using
a combination of --base / --days / --pattern / --index will be polled for ok status.

=item B<max-segments>

The more segments in a shard, the slower searches will be against that shard.
This option requires the --index flag in order to establish the correct sets of
shards to check for segmentation.

=back

=head1 DESCRIPTION

This is a plugin to poll elasticsearch for it's status and report back to Nagios.

=cut
