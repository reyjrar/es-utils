package App::ElasticSearch::Utilities::QueryString::FileExpansion;
# ABSTRACT: Build a terms query from unique values in a column of a file

use v5.16;
use warnings;

# VERSION

use CLI::Helpers qw(:output);
use File::Slurp::Tiny qw(read_lines);
use JSON::MaybeXS;
use Ref::Util qw(is_ref is_arrayref is_hashref);
use Text::CSV_XS;
use namespace::autoclean;

use Moo;
with 'App::ElasticSearch::Utilities::QueryString::Plugin';

sub _build_priority { 10; }

my %parsers = (
    txt  => \&_parse_txt,
    dat  => \&_parse_txt,
    csv  => \&_parse_csv,
    json => \&_parse_json,
);

=for Pod::Coverage handle_token

=cut

sub handle_token {
    my($self,$token) = @_;

    my $makeMatcher = sub {
        my ($matcher,$field,$patterns)  = @_;
        my @tests;
        foreach my $pattern (@{ $patterns }) {
            push @tests, { $matcher => { $field => { value => $pattern } } };
        }
        return {
            bool => {
                should => \@tests,
                minimum_should_match => 1,
            }
        }
    };
    my %make = (
        terms => sub {
            my ($field, $uniq) = @_;
            return { terms => { $field => $uniq } };
        },
        regexp   => sub { $makeMatcher->(regexp   => @_) },
        wildcard => sub { $makeMatcher->(wildcard => @_) },
    );
    if( my ($term,$match) = split /\:/, $token, 2 ) {
        if( defined $match && $match =~ /(.*\.(\w{3,4}))(?:\[([^\]]+)\])?$/) {
            my($file,$type,$col) = ($1,$2,$3);
            # Support Wildcards
            my $matcher = $file =~ s/^\~// ? 'regexp'
                        : $file =~ s/^\*// ? 'wildcard'
                        : 'terms';
            $col //= -1;
            $type = lc $type;
            verbose({level=>2,color=>'magenta'}, sprintf "# %s attempt of %s type, %s[%s] %s",
                $self->name, $type, $file, $col, -f $file ? 'exists' : 'does not exist'
            );
            if( exists $parsers{$type} && -f $file ) {
                my $uniq = $parsers{$type}->($file,$col);
                if (defined $uniq && is_hashref($uniq) && scalar(keys %$uniq)) {
                    verbose({color=>'cyan'},
                        sprintf "# FILE:%s[%s] contained %d unique elements.",
                        $file,
                        $col,
                        scalar(keys %$uniq),
                    );
                    my $qs = [ sort keys %{ $uniq } ];
                    return [{condition => $make{$matcher}->($term,$qs) }];
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
            # Split on tabs or nulls
            my @cols = split /[\t\0]/;
            my $value = $cols[$col];
            if(defined $value) {
                $uniq{$value} = 1;
            }
        }
    }
    return \%uniq;
}

sub _parse_json {
    my ($file,$field) = @_;

    die "For new line delimited JSON, please specify the key, ie <field>:$file\[key.path.i.want\]"
        if $field eq "-1";

    my %uniq = ();
    my $line = 0;
    my @path = split /\./, $field;      # Supports key.subkey.subsubkey format
    JSON_LINE: foreach my $json ( read_lines($file) ) {
        $line++;
        my $data;
        eval {
            $data = decode_json($json);
            1;
        } or do {
            my $err = $@;
            output({stderr=>1,color=>'yellow'}, sprintf "Invalid JSON in %s, line %d: %s",
                $file,
                $line,
                $err,
            );
            verbose({stderr=>1,color=>'magenta',indent=>1}, $json);
            next;
        };
        # Walk the path
        foreach my $k (@path) {
            next JSON_LINE unless exists $data->{$k};
            $data = $data->{$k};
        }
        # At this point $data should contain our values
        if( is_arrayref($data) ) {
            $uniq{$_} = 1 for grep { !is_ref($_) } @{ $data };
        }
        elsif( !is_ref($data) ) {
            $uniq{$data} = 1;
        }
    }

    die "Expected newline-delimited JSON in $file, but it was empty or didn't contain '$field'"
        unless keys %uniq;

    return \%uniq;
}

1;
__END__

=head1 SYNOPSIS

=head2 App::ElasticSearch::Utilities::QueryString::FileExpansion

If the match ends in .dat, .txt, .csv, or .json then we attempt to read a file with that name and OR the condition:

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

Or

    $ cat test.json
    { "ip": "1.2.3.4" }
    { "ip": "1.2.3.5" }
    { "ip": "1.2.3.6" }
    { "ip": "1.2.3.7" }


We can source that file:

    src_ip:test.dat      => src_ip:(1.2.3.4 1.2.3.5 1.2.3.6 1.2.3.7)
    src_ip:test.json[ip] => src_ip:(1.2.3.4 1.2.3.5 1.2.3.6 1.2.3.7)

This make it simple to use the --data-file output options and build queries
based off previous queries. For .txt and .dat file, the delimiter for columns
in the file must be either a tab or a null.  For files ending in
.csv, Text::CSV_XS is used to accurate parsing of the file format.  Files
ending in .json are considered to be newline-delimited JSON.

You can also specify the column of the data file to use, the default being the last column or (-1).  Columns are
B<zero-based> indexing. This means the first column is index 0, second is 1, ..  The previous example can be rewritten
as:

    src_ip:test.dat[1]

or:
    src_ip:test.dat[-1]

For newline delimited JSON files, you need to specify the key path you want to extract from the file.  If we have a
JSON source file with:

    { "first": { "second": { "third": [ "bob", "alice" ] } } }
    { "first": { "second": { "third": "ginger" } } }
    { "first": { "second": { "nope":  "fred" } } }

We could search using:

    actor:test.json[first.second.third]

Which would expand to:

    { "terms": { "actor": [ "alice", "bob", "ginger" ] } }

This option will iterate through the whole file and unique the elements of the list.  They will then be transformed into
an appropriate L<terms query|http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html>.

=head3 Wildcards

We can also have a group of wildcard or regexp in a file:

    $ cat wildcards.dat
    *@gmail.com
    *@yahoo.com

To enable wildcard parsing, prefix the filename with a C<*>.

    es-search.pl to_address:*wildcards.dat

Which expands the query to:

    {
      "bool": {
        "minimum_should_match":1,
        "should": [
           {"wildcard":{"to_outbound":{"value":"*@gmail.com"}}},
           {"wildcard":{"to_outbound":{"value":"*@yahoo.com"}}}
        ]
      }
    }

No attempt is made to verify or validate the wildcard patterns.

=head3 Regular Expressions

If you'd like to specify a file full of regexp, you can do that as well:

    $ cat regexp.dat
    .*google\.com$
    .*yahoo\.com$

To enable regexp parsing, prefix the filename with a C<~>.

    es-search.pl to_address:~regexp.dat

Which expands the query to:

    {
      "bool": {
        "minimum_should_match":1,
        "should": [
          {"regexp":{"to_outbound":{"value":".*google\\.com$"}}},
          {"regexp":{"to_outbound":{"value":".*yahoo\\.com$"}}}
        ]
      }
    }

No attempt is made to verify or validate the regexp expressions.
