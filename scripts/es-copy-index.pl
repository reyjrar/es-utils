#!/usr/bin/env perl
# PODNAME: es-copy-index.pl
# ABSTRACT: Copy an index from one cluster to another

use strict;
use warnings;

use Carp;
use Getopt::Long qw(:config posix_default no_ignore_case no_ignore_case_always);
use Pod::Usage;

use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(:default :index);

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT,
    'from:s',
    'to:s',
    'rename:s',
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $OPT{manual};
my $index = shift @ARGV;

my %INDEX = (
    from => $index,
    to   => exists $OPT{rename} && length $OPT{rename} ? $OPT{rename} : $index,
);

# Check for valid use cases
if ( !defined($index) || !exists $OPT{from} || !exists $OPT{to} ) {
    pod2usage(-exitstatus => 1, -verbose => 2);
}

# Connect to ElasticSearch
my %ES = ();
foreach my $dir (qw(from to)) {
    $ES{$dir} = es_connect( [ "$OPT{$dir}:9200" ] );
}

croak "Invalid index: $index\n" unless $ES{from}->exists( index => $INDEX{from} );

croak "Index $INDEX{to} already exists in $OPT{to}\n" if $ES{to}->exists( index => $INDEX{to} );

my $RECORDS = 0;
my $LAST = time;
my ($status, $res);

$res = $ES{from}->get( index => $INDEX{from} );
my $settings = $res->{$index}{settings};
my $mappings = $res->{$index}{mappings};

($status, $res) = $ES{to}->put(
    index => $INDEX{to},
    body  => {
        settings => {
            index => {
                number_of_shards => $settings->{index}{number_of_shards},
                number_of_replicas => $settings->{index}{number_of_replicas},
            }
        },
        mappings => $mappings,
    }
);

require JSON;
my $JSON = JSON->new->pretty->canonical;
if ($status ne "200") {
    croak "Failed to create index in $OPT{to} (http status = $status): " . $JSON->encode([ $status, $res ]);
}

$ES{from}->scan_scroll(
    index => $INDEX{from},
    body => { size => 1000 },
    on_response => sub {
        my ($status, $res) = @_;

        my $body = [
            map {(
                { create => { _type => $_->{_type},  _id => $_->{_id}, } },
                $_->{_source}
            )} @{ $res->{hits}{hits} }
        ];
        my ($s2, $r2) = $ES{to}->bulk(
            index => $INDEX{to},
            body => $body
        );

        if ($status ne "200") {
            croak "Failed to put documents to $OPT{to} (http status = $status): " . $JSON->encode([ $s2, $r2 ]);
        }

        show_counts( scalar @{$res->{hits}{hits}} );

        return 1;
    }
);

print "Optimizing $INDEX{to}.\n";
$ES{to}->post(
    index   => $INDEX{to},
    command => "_optimize",
    body => {
        max_num_segments => 1,
        wait_for_merge   => 0,
    }
);

sub show_counts {
    my $inc_records = shift;

    output({color=>'green'}, "Starting copy of $INDEX{from} to $OPT{to}:$INDEX{to}.") if $RECORDS == 0;

    $RECORDS += $inc_records;
    if( $RECORDS % 100_000 == 0 ) {
        my $now = time;
        my $diff = $now - $LAST;
        my @time=localtime;
        my $msg = sprintf "%02d:%02d:%02d Records: %d (diff:%0.2f)\n", @time[2,1,0], $RECORDS, $diff;
        output({color=>'yellow'}, $msg);
        $LAST=$now;
    }
}

__END__

=head1 NAME

es-copy-index.pl - Copy an index from one cluster to another

=head1 SYNOPSIS

es-copy-access.pl [index]

Options:

    --from              A server in the cluster where the index lives
    --to                A server in the cluster where the index will be copied to
    --rename            Change the name of the index on the destination
    --help              print help
    --manual            print full manual

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<from>

B<REQUIRED>: hostname or IP of the source cluster

=item B<to>

B<REQUIRED>: hostname or IP of the destination cluster

=item B<rename>

Optional: change the name of the index on the destination cluster


=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=back

=head1 DESCRIPTION

This uses the reindex API to copy data from one cluster to another

Example:

   es-copy-index.pl --from localhost --to remote.cluster.com logstash-2013.01.11

=cut

