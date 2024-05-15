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
);
my ($opt,$usage) = describe_options('%c %o',
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

#------------------------------------------------------------------------#
my $json = JSON->new->pretty->utf8->canonical;

my %indices = map { $_ => (es_index_days_old($_) || 0) } es_indices();

const my @FieldStores => qw(
    doc_values
    norms
    stored_fields
    term_vectors
    points
);

my %Fields = ();

my $maxlen = (sort map  { length  } keys %indices)[-1];

foreach my $idx ( sort keys %indices ) {
    # Get Index Stats
    if( my $result = es_index_stats($idx) ) {
        my $total = $result->{_all}{total};
        my $segment_ratio = $total->{segments}{count} / $total->{shard_stats}{total_count};
        my $color = $segment_ratio > 2 ? 'red'
                  : $segment_ratio > 1 ? 'yellow'
                  : 'green';
        output({data=>1,color=>$color}, sprintf "%${maxlen}s %s total (segment ratio: %0.2f), per doc: %s",
            $idx,
            es_human_size($total->{store}{size_in_bytes}),
            $segment_ratio,
            $total->{docs}{count} > 0 ?
                es_human_size($total->{store}{size_in_bytes} / $total->{docs}{count}) :
                0
        );
    }
}

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
