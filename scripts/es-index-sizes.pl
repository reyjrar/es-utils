#!perl
# PODNAME: es-index-fields.pl
# ABSTRACT: Show information on the fields storage usage
use strict;
use warnings;

use App::ElasticSearch::Utilities qw(:all);
use CLI::Helpers qw(:all);
use Const::Fast;
use JSON::MaybeXS;
use Getopt::Long::Descriptive;
use Pod::Usage;

#------------------------------------------------------------------------#
# Argument Collection
const my %DEFAULT => (
    store => 'primaries',
);
my ($opt,$usage) = describe_options('%c %o',
    ['store|s=s', "Show stats about primaries or total, defaults to $DEFAULT{store}",
        { default => $DEFAULT{store} }
    ],
    []     ,
    ['help', 'Display this message', { shortcircuit => 1 }],
    ['manual', 'Display full manual', { shortcircuit => 1 }],
);

#------------------------------------------------------------------------#
# Documentations!
if( $opt->help ) {
    print $usage->text;
    exit 0;
}
pod2usage(-exitstatus => 0, -verbose => 2) if $opt->manual;

my %stores = map { $_ => 1 } qw( primaries total );
die sprintf("Bad argument --stores, must be one of: %s", join ', ', sort keys %stores)
    unless $stores{$opt->store};

#------------------------------------------------------------------------#
my $json = JSON->new->pretty->utf8->canonical;

my @indices = es_indices();

const my @FieldStores => qw(
    doc_values
    norms
    stored_fields
    term_vectors
    points
);

my %Fields = ();

my $maxlen = (sort map { length } @indices)[-1];

my %days = ();
my $total_size = 0;
foreach my $idx ( sort @indices ) {
    # Get Index Stats
    if( my $result = es_index_stats($idx) ) {
        my $total = $result->{_all}{$opt->store};
        my $segment_ratio = $total->{segments}{count} / $total->{shard_stats}{total_count};
        my $color = $segment_ratio > 2 ? 'red'
                  : $segment_ratio > 1 ? 'yellow'
                  : 'green';
        my $index_data = sprintf("%${maxlen}s %s total (segment ratio: %0.2f), per doc: %s",
            $idx,
            es_human_size($total->{store}{size_in_bytes}),
            $segment_ratio,
            $total->{docs}{count} > 0 ?
                es_human_size($total->{store}{size_in_bytes} / $total->{docs}{count}) :
                0
        );

        $total_size += $total->{store}{size_in_bytes};
        if( my @parts = ($idx =~ /\b([0-9]{4})(?:\.|-)?([0-9]{2})(?:\.|-)?([0-9]{2})/) ) {
            verbose({color=>$color}, $index_data);
            my $date = join '-', @parts;
            $days{$date} += $total->{store}{size_in_bytes};
        }
        else {
            output({data=>1,color=>$color}, $index_data);
        }
    }
}

foreach my $date ( sort keys %days ) {
    output({data=>1},
        sprintf "Index totals of %s for %s: %s",
            $opt->store,
            $date,
            es_human_size($days{$date})
    );
}

output(sprintf "Total storage size of %s: %s",
    $opt->store,
    es_human_size($total_size)
);

__END__

=head1 SYNOPSIS

es-index-sizes.pl --base logs

Options:


    --help              print help
    --manual            print full manual

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 DESCRIPTION

This script allows you to see index sizes comparatively

=cut
