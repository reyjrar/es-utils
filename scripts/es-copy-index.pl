#!/usr/bin/env perl
# PODNAME: es-copy-index.pl
# ABSTRACT: Copy an index from one cluster to another

use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:default :index);
use App::ElasticSearch::Utilities::Query;
use App::ElasticSearch::Utilities::QueryString;
use CLI::Helpers qw(:all);
use File::Slurp::Tiny qw(read_lines);
use Getopt::Long qw(:config posix_default no_ignore_case no_ignore_case_always);
use Hash::Merge::Simple qw(clone_merge);
use JSON::XS;
use Pod::Usage;
use Time::HiRes qw(time);

#------------------------------------------------------------------------#
# Argument Parsing
my %OPT;
GetOptions(\%OPT, qw(
    from=s
    to:s
    source=s
    destination:s
    append|A
    block:1000
    mapping:s
    settings:s
    help|h
    manual|m
));

#------------------------------------------------------------------------#
# Documentation
pod2usage(1) if $OPT{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $OPT{manual};
debug_var(\%OPT);

#------------------------------------------------------------------------#
# Copy To/From
my %INDEX = (
    from => $OPT{source},
    to   => exists $OPT{destination} ? $OPT{destination} : $OPT{source},
);
my %HOST = (
    from => $OPT{from},
    to   => exists $OPT{to} ? $OPT{to} : $OPT{from},
);
if( $HOST{to} eq $HOST{from} && $INDEX{to} eq $INDEX{from} ) {
    output({color=>'red',IMPORTANT=>1},
        "FATAL: Cannot copy from the same host to the same index name!"
    );
    exit 1;
}

#------------------------------------------------------------------------#
# Build the Query
my $JSON = JSON::XS->new->pretty->canonical;
my $qs = App::ElasticSearch::Utilities::QueryString->new();
my $q  = @ARGV ? $qs->expand_query_string(@ARGV)
               : App::ElasticSearch::Utilities::Query->new(must => {match_all=>{}});

$q->set_scan_scroll('1m');
$q->set_size( $OPT{'block'} );

# Connect to ElasticSearch
my %ES = ();
foreach my $dir (qw(from to)) {
    $ES{$dir} = es_connect( [ "$HOST{$dir}:9200" ] );
}

die "Invalid index: $INDEX{from}" unless $ES{from}->exists( index => $INDEX{from} );
my $TO_EXISTS = $ES{to}->exists( index => $INDEX{to}  );

my $RECORDS = 0;
my $TOTAL=0;
my $LAST = time;
my ($status, $res);

# Mappings/Settings for Non-existant index.
unless( exists $OPT{append} ) {
    die "Index $INDEX{to} already exists in $HOST{to}" if $TO_EXISTS;
    $res = es_request($ES{from}, '_settings', {index => $INDEX{from}} );
    debug_var($res);
    my $from_settings = $res->{$INDEX{from}}{settings};
    my @settings = ({
        index => {
            number_of_shards   => $from_settings->{index}{number_of_shards},
            number_of_replicas => $from_settings->{index}{number_of_replicas},
        }
    });
    if( exists $OPT{settings} && -f $OPT{settings} ) {
        my $content = join '', read_lines($OPT{settings});
        eval {
            push @settings, $JSON->decode($content);
            1;
        } or do {
            debug($content);
            die "Parsing JSON from $OPT{settings} failed: $@";
        };
    }
    my $to_settings = clone_merge(@settings);

    # Determine if we get mappings from a file or from the index.
    my $mappings;
    if( exists $OPT{mapping} && -f $OPT{mapping} ) {
        my $content = join '', read_lines($OPT{mapping});
        eval {
            $mappings = $JSON->decode($content);
            1;
        } or do {
            debug($content);
            die "Parsing JSON from $OPT{mapping} failed: $@";
        };
    }
    else {
        $mappings = $res->{$INDEX{from}}{mappings};
    }

    ($status, $res) = $ES{to}->put(
        index => $INDEX{to},
        body  => {
            settings => $to_settings,
            mappings => $mappings,
        }
    );

    if ($status ne "200") {
        die "Failed to create index in $HOST{to} (http status = $status): " . $JSON->encode([ $status, $res ]);
    }
}
else {
    my @ignored=();
    foreach my $k (qw(settings mapping)) {
        push @ignored, $k if exists $OPT{$k} && -f $OPT{$k};
    }
    output({color=>'warning',sticky=>1},
        sprintf "%s - warning ignoring %s as they are invalid in this context.", basename($0), join(', ', map { "--$_" } @ignored)
    ) if @ignored;
} # End Mappings/Settings for Non-existant index.

$res = es_request($ES{from}, '_search',
    # Search Parameters
    {
        index     => $INDEX{from},
        uri_param => $q->uri_params,
        method => 'GET',
    },
    # Search Body
    $q->request_body,
);
debug_var($q->request_body);
debug_var($res);

while( $res && @{ $res->{hits}{hits} }) {
    $TOTAL ||= $res->{hits}{total};
    my $start=time;
    my $batch=0;
    my $body = [
        map {
            $batch++;
            (
                { create => { _type => $_->{_type},  _id => $_->{_id}, } },
                $_->{_source}
            )
        } @{ $res->{hits}{hits} }
    ];
    my $max_retries = 3;
    my $success = 0;
    while ($max_retries--) {
        my ($s2, $r2) = $ES{to}->bulk(
            index => $INDEX{to},
            body => $body
        );
        if ($s2 ne "200") {
            output({stderr=>1,color=>'red'},"Failed to put documents to $HOST{to} (http status = $status): " . $JSON->encode([ $s2, $r2 ]));
            next;
        }
        $success=1;
        last;
    }
    die "Failed to write data to $HOST{to}:9200/$INDEX{to} after $RECORDS docs indexed."
        unless $success;
    my $took = time - $start;
    show_counts( scalar @{$res->{hits}{hits}} );
    $res = es_request('_search/scroll', {
        uri_param => {
            scroll_id => $res->{_scroll_id},
            scroll    => '1m',
        }
    });

    verbose(sprintf "Batch of %d done in %00.2fds.", $batch, $took);
}

sub show_counts {
    my $inc_records = shift;

    output({color=>'green'}, "Starting copy of $INDEX{from} to $HOST{to}:$INDEX{to}.") if $RECORDS == 0;

    $RECORDS += $inc_records;
    if( $RECORDS % ($OPT{block} * 10) == 0 ) {
        my $now = time;
        my $diff = $now - $LAST;
        my @time=localtime;
        my $msg = sprintf "%00.2f%% %02d:%02d:%02d Records: %d of %d in %0.2fs", ($RECORDS/$TOTAL)*100, @time[2,1,0], $RECORDS, $TOTAL, $diff;
        output({color=>'yellow'}, $msg);
        $LAST=$now;
    }
}

__END__

=head1 NAME

es-copy-index.pl - Copy an index from one cluster to another

=head1 SYNOPSIS

es-copy-access.pl [options] [query to select documents]

Options:

    --source            (Required) The source index name for the copy
    --destination       Destination index name, assumes source
    --from              (Required) A server in the cluster where the index lives
    --to                A server in the cluster where the index will be copied to
    --block             How many docs to process in one batch, default: 1,000
    --mapping           JSON mapping to use instead of the source mapping
    --settings          JSON index settings to use instead of those from the source
    --append            Instead of creating the index, add the documents to the destination
    --help              print help
    --manual            print full manual

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<from>

B<REQUIRED>: hostname or IP of the source cluster

=item B<to>

Hostname or IP of the destination cluster, defaults to the same host unless otherwise specified.

=item B<source>

B<REQUIRED>: name of the source index for the copy

=item B<destination>

Optional: change the name of the index on the destination cluster

=item B<block>

Batch size of docs to process in one retrieval, default is 1,000

=item B<mapping>

Path to a file containing JSON mapping to use on the destination index
instead of the mapping directly from the source index.

=item B<settings>

Path to a file containing JSON settings to use on the destination index
instead of the settings directly from the source index.

=item B<append>

This mode skips the index mapping and settings configuration and just being indexing
documents from the source into the destination.

=item B<help>

Print this message and exit

=item B<manual>

Print detailed help with examples

=back

=head1 DESCRIPTION

This script allows you to copy data from one index to another on the same cluster or
on a separate cluster.  It handles index creation, either directly copying the mapping
and settings from the source index or from mapping/settings JSON files.

This script could also be used to split up an index into smaller indexes for any number of reasons.

This uses the reindex API to copy data from one cluster to another


=head1 EXAMPLES

=head2 Copy to different cluster

   es-copy-index.pl --from localhost --to remote.cluster.com --source logstash-2013.01.11

=head2 Rename an existing index

   es-copy-index.pl --from localhost --source logstash-2013.01.11 --destination logs-2013.01.11

=head2 Subset an existing index

   es-copy-index.pl --from localhost \
        --source logstash-2013.01.11 \
        --destination secure-2013.01.11 \
        category:'(authentication authorization)'

=head2 Changing settings and mappings

   es-copy-index.pl --from localhost \
        --source logstash-2013.01.11 \
        --destination testing-new-settings-old-data-2013.01.11 \
        --settings new_settings.json \
        --mappings new_mappings.json


=head2 Building an Incident Index using append

Let's say we were investigating an incident and wanted to have
an index that contained the data we were interested in.  We could use different
retention rules for incident indexes and we could arbitrarily add data to them based
on searches being performed on the source index.

Here's our initial query, a bad actor on our admin login page.

   es-copy-index.pl --from localhost \
        --source logstash-2013.01.11 \
        --destination incident-rt1234-2013.01.11 \
        src_ip:1.2.3.4 dst:admin.exmaple.com and file:'\/login.php'

Later on, we discover there was another actor:

   es-copy-index.pl --from localhost \
        --source logstash-2013.01.11 \
        --destination incident-rt1234-2013.01.11 \
        --append \
        src_ip:4.3.2.1 dst:admin.exmaple.com and file:'\/login.php'

The B<incident-rt1234-2013.01.11> index will now hold all the data from both of those queries.

=head1 Query Syntax Extensions

=from_other App::ElasticSearch::Utilities::QueryString / Extended Syntax / all

=cut

