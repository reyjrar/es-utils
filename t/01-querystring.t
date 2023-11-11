#!perl
#
use strict;
use warnings;

use App::ElasticSearch::Utilities::QueryString;
use CLI::Helpers qw(:output);
use Data::Dumper;
use File::Temp qw(tempfile);
use Test::More;

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;

# Test Data File;
my ($fh,$tempfile) = tempfile(SUFFIX => '.dat');
my @csv_data=();
while(<DATA>) {
    chomp;
    my @cols = split /\s+/, $_;
    printf $fh "%s\n", join("\t", @cols);
    push @csv_data, [ @cols ];
}
close($fh);
my ($csv,$csvfile) = tempfile( SUFFIX => '.csv' );
for(@csv_data) {
    printf $csv "%s\n", join(',', @{ $_ });
}
close($csv);
END {
    debug({color=>'red'},"[test_cleanup] Removing $tempfile");
    unlink($tempfile);
    debug({color=>'red'},"[test_cleanup] Removing $csvfile");
    unlink($csvfile);
}
# Query String Parser Testing
my %tests = (
    '00-barewords' => [
        [qw(src_ip:1.2.3.4 and not dst:www.example.com)],
        {
            'bool' => {
                'must' => [
                    {
                        'query_string' => {
                            'query' => 'src_ip:1.2.3.4 AND NOT dst:www.example.com'
                        }
                    }
                ]
            }
        },
    ],
    '01-ip-cidr-expansion' => [
        [qw(src_ip:10.0.0.0/8)],
        {
          'bool' => {
            'must' => [
              {
                'range' => {
                  'src_ip' => { gte => '10.0.0.0', lte => '10.255.255.255' }
                }
              }
            ]
          }
        },
    ],
    '02-underscored' => [
        ["_prefix_:useragent=Go "],
        {
          'bool' => {
            'must' => [
              {
                'prefix' => {
                  'useragent' => 'Go '
                }
              }
            ]
          }
        },
    ],
    '03-file-expansion-dat' => [
        [sprintf "src_ip:%s[-1]", $tempfile],
        {
          'bool' => {
            'must' => [
              {
                'terms' => {
                  'src_ip' => [
                    '1.2.3.4',
                    '1.2.3.5',
                    '1.2.3.6'
                  ]
                }
              }
            ]
          }
        },
    ],
    '04-file-expansion-csv' => [
        [sprintf "src_ip:%s[-1]", $csvfile],
        {
          'bool' => {
            'must' => [
              {
                'terms' => {
                  'src_ip' => [
                    '1.2.3.4',
                    '1.2.3.5',
                    '1.2.3.6'
                  ]
                }
              }
            ]
          }
        },
    ],
    '05-dangling-words' => [
        [qw(and not username:bob and)],
        {
          'bool' => {
            'must' => [
              {
                'query_string' => {
                  'query' => 'NOT username:bob'
                }
              }
            ]
          }
        },
    ],
    '06-dangling-words' => [
        [qw(username:bob and _prefix_:useragent:Godzilla)],
        {
          'bool' => {
            'must' => [
              {
                'prefix' => {
                  'useragent' => 'Godzilla'
                }
              },
              {
                'query_string' => {
                  'query' => 'username:bob'
                }
              }
            ]
          }
        },
    ],
    '07-term-filter' => [
        [qw(=foo:bar)],
        {
          'bool' => {
            'must' => [
              {
                'term' => {
                  'foo' => 'bar',
                }
              }
            ]
          }
        },
    ],
    '08-fuzzy-filter' => [
        [qw(~foo:bar)],
        {
          'bool' => {
            'must' => [
              {
                'fuzzy' => {
                  'foo' => 'bar',
                }
              }
            ]
          }
        },
    ],
    '08-wildcard-filter' => [
        [qw(*foo:bar*)],
        {
          'bool' => {
            'must' => [
              {
                'wildcard' => {
                  'foo' => 'bar*',
                }
              }
            ]
          }
        },
    ],
    '09-regexp-filter' => [
        [qw(/foo:bar.*)],
        {
          'bool' => {
            'must' => [
              {
                'regexp' => {
                  'foo' => 'bar.*',
                }
              }
            ]
          }
        },
    ],
    '10-phrase-filter' => [
        ['+foo:bar baz'],
        {
          'bool' => {
            'must' => [
              {
                'match_phrase' => {
                  'foo' => 'bar baz',
                }
              }
            ]
          }
        },
    ],
    '11-match-promotion' => [
        ['a_text_field:foo'],
        {
          'bool' => {
            'must' => [
              {
                'match' => {
                  'a_text_field' => 'foo',
                }
              }
            ]
          }
        },
    ],
    '12-match-not-terms' => [
        ['=a_text_field:foo'],
        {
          'bool' => {
            'must' => [
              {
                'match' => {
                  'a_text_field' => 'foo',
                }
              }
            ]
          }
        },
    ],
);

my $qs = App::ElasticSearch::Utilities::QueryString->new(
    fields_meta => {
        a_text_field => { type => "text" },
    },
);

foreach my $t (sort keys %tests) {
    my $q = $qs->expand_query_string( @{ $tests{$t}->[0] } );

    is_deeply( $q->query, $tests{$t}->[1], $t )
        or diag( join(' ', @{ $tests{$t}->[0] }) . "\n", Dumper $q->query );
}
done_testing();
__DATA__
alice   50      1.2.3.4
bob     20      1.2.3.5
charlie 70      1.2.3.6
