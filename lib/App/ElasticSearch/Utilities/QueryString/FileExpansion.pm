package App::ElasticSearch::Utilities::QueryString::FileExpansion;
# ABSTRACT: Build a terms query from unique values in a column of a file

use strict;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use File::Slurp::Tiny qw(read_lines);
use Ref::Util qw(is_hashref);
use Text::CSV_XS;
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 10; }

my %parsers = (
    txt => \&_parse_txt,
    dat => \&_parse_txt,
    csv => \&_parse_csv,
);

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my($self,$token) = @_;

    if( my ($term,$match) = split /\:/, $token, 2 ) {
        if( defined $match && $match =~ /(.*\.(\w{3,4}))(?:\[(-?\d+)\])?$/) {
            my($file,$type,$col) = ($1,$2,$3);
            $col //= -1;
            $type = lc $type;
            verbose({level=>2,color=>'magenta'}, sprintf "# %s attempt of %s type, %s[%d] %s",
                $self->name, $type, $file, $col, -f $file ? 'exists' : 'does not exist'
            );
            if( exists $parsers{$type} && -f $file ) {
                my $uniq = $parsers{$type}->($file,$col);
                if (defined $uniq && is_hashref($uniq) && scalar(keys %$uniq)) {
                    verbose({color=>'cyan'},
                        sprintf "# FILE:%s[%d] contained %d unique elements.",
                        $file,
                        $col,
                        scalar(keys %$uniq),
                    );
                    return [{condition => {terms => {$term => [sort keys %$uniq]}}}];
                }
            }
        }
    }
    return;
}

sub _parse_csv {
    my ($file,$col) = @_;
    my $csv = Text::CSV_XS->new({binary=>1,empty_is_undef=>1});
    open my $fh, "<:encoding(utf8)", $file or die "Unable to read $file: $!";
    my %uniq = ();
    while( my $row = $csv->getline($fh) ) {
        my $val;
        eval {
            $val = $row->[$col];
        };
        next unless defined $val;
        $uniq{$val} = 1;
    }
    return \%uniq;
}

sub _parse_txt {
    my ($file,$col) = @_;
    my %uniq=();
    my @rows = grep { defined && length && !/^#/ } read_lines($file);
    debug({color=>'magenta'}, @rows);
    if(@rows) {
        for(@rows) {
            chomp;
            my @cols = split /[\s,]+/;
            my $value = $cols[$col];
            if(defined $value) {
                $uniq{$value} = 1;
            }
        }
    }
    return \%uniq;
}

1;
__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::QueryString::FileExpansion

If the match ends in .dat, .txt, or .csv, then we attempt to read a file with that name and OR the condition:

    $ cat test.dat
    50  1.2.3.4
    40  1.2.3.5
    30  1.2.3.6
    20  1.2.3.7

Or

    $ cat test.csv
    50,1.2.3.4
    40,1.2.3.5
    30,1.2.3.6
    20,1.2.3.7

Or

    $ cat test.txt
    1.2.3.4
    1.2.3.5
    1.2.3.6
    1.2.3.7

We can source that file:

    src_ip:test.dat => src_ip:(1.2.3.4 1.2.3.5 1.2.3.6 1.2.3.7)

This make it simple to use the --data-file output options and build queries
based off previous queries. For .txt and .dat file, the delimiter for columns
in the file must be either a tab, comma, or a semicolon.  For files ending in
.csv, Text::CSV_XS is used to accurate parsing of the file format.

You can also specify the column of the data file to use, the default being the last column or (-1).  Columns are
B<zero-based> indexing. This means the first column is index 0, second is 1, ..  The previous example can be rewritten
as:

    src_ip:test.dat[1]

or:
    src_ip:test.dat[-1]

This option will iterate through the whole file and unique the elements of the list.  They will then be transformed into
an appropriate L<terms query|http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html>.
