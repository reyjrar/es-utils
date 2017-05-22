#!/usr/bin/env perl
# PODNAME: es-alias-manager.pl
# ABSTRACT: Allow easy alias management for daily indexes
use strict;
use warnings;

use DateTime;
use YAML;
use Getopt::Long qw(:config no_ignore_case no_ignore_case_always);
use Pod::Usage;
use CLI::Helpers qw(:all);
use App::ElasticSearch::Utilities qw(:default);

#------------------------------------------------------------------------#
# Argument Collection
my %opt;
GetOptions(\%opt,
    'all',
    'config:s',
    'skip:s',
    # Basic options
    'help|h',
    'manual|m',
);

#------------------------------------------------------------------------#
# Documentations!
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{manual};

my %actions = (
    add => 'Create any missing aliases',
    remove => 'Remove any aliases not in the desired set',
);

# We might skip one thing or another
if( exists $opt{skip} && !exists $actions{$opt{skip}} ) {
    output({color=>'red',sticky=>1}, "Invalid action to skip: $opt{skip}");
    output({clear=>1},"Valid actions to skip are:");
    output({indent=>1}, sprintf "%s - %s", $_, $actions{$_}) for sort keys %actions;
    pod2usage(-exitstatus => 1);
}

my %CFG = (
    config => '/etc/elasticsearch/aliases.yml',
);
# Extract from our options if we've overridden defaults
foreach my $setting (keys %CFG) {
    $CFG{$setting} = $opt{$setting} if exists $opt{$setting} and defined $opt{$setting};
}
if ( !exists $CFG{config} and ! -f $CFG{config} ) {
    pod2usage(1);
}
my $ALIAS = YAML::LoadFile( $CFG{config} ) or die "unable to read $CFG{config}";

# Create the target uri for the ES Cluster
my $TARGET = exists $opt{host} && defined $opt{host} ? $opt{host} : 'localhost';

# Grab a connection to ElasticSearch
my $es = es_connect();

# Delete Indexes older than a certain point
my $TODAY = DateTime->now()->truncate( to => 'day' );
my $indices = es_request('_aliases');

if ( !defined $indices ) {
    output({color=>"red"}, "Unable to locate indices by get_aliases()!");
    exit 1;
}
debug_var($indices);

my %PARTS = (
    DATE => {
        RE  => '(?<year>\d{4})[.\-](?<month>\d{2})[.\-](?<day>\d{2})',
        FMT => join('.', '%Y', '%m', '%d'),
    },
    PERIOD => {
        FMT => '%s',
    }
);

foreach my $base (keys %{ $ALIAS }) {
    my $re = $ALIAS->{$base}{pattern};
    $re =~ s/[^\w{}*?-]+//g;

    # Wildcards
    $re =~ s/\*+/.\+/g;
    $re =~ s/\?/./g;

    # Varaibles
    $re =~ s/\{\{([^\}]+)\}\}/$PARTS{$1}->{RE}/g;
    $ALIAS->{$base}{re} = qr/$re/;

    # Setup Formatting
    if (exists $ALIAS->{$base}{daily}) {
        $ALIAS->{$base}{daily} =~ s/\{\{([^\}]+)\}\}/$PARTS{$1}->{FMT}/g;
    }
    if (exists $ALIAS->{$base}{relative}) {
        $ALIAS->{$base}{relative}{alias} =~ s/\{\{([^\}]+)\}\}/$PARTS{$1}->{FMT}/g;
        while( my ($period,$def) =  each %{ $ALIAS->{$base}{relative}{periods} }) {
            my %dt = (
                to => $TODAY->clone(),
                from => exists $def->{from} ? $TODAY->clone() : DateTime->from_epoch(epoch => 0)->truncate( to => 'day'),
            );
            debug("Period[$period] subtracting: ");
            debug_var($def);
            foreach my $d (keys %dt) {
                if ( exists $def->{$d} ) {
                    $dt{$d}->subtract( %{ $def->{$d}} );
                    debug("$period $d " . $dt{$d}->ymd);
                }
            }
            $ALIAS->{$base}{relative}{periods}{$period} = \%dt;
        }
    }
}
debug("Aliases being applied:");
debug_var($ALIAS);

# Loop through the indices and take appropriate actions;
foreach my $index (sort keys %{ $indices }) {
    debug("$index being evaluated");
    my %current = %{ $indices->{$index}{aliases}};
    my $managed = 0;

    my %desired = ();
    while( my($name,$map) = each %{ $ALIAS }) {
        if ($index =~ /$map->{re}/) {
            $managed++;
            my $idx_dt = DateTime->new( map { $_ => $+{$_} } qw(year month day) );
            verbose("$index is a $name index.");

            if ( exists $map->{daily} ) {
                my $daily = $idx_dt->strftime($map->{daily});
                $desired{$daily} = 1;
            }
            if ( exists $map->{relative} ) {
                while (my ($period,$def) = each %{ $map->{relative}{periods} }) {
                    debug(sprintf("Checking index date (%s) is between %s and %s",
                            $idx_dt->ymd,
                            $def->{from}->ymd,
                            $def->{to}->ymd,
                        ));
                    if( $idx_dt <= $def->{to} && $idx_dt >= $def->{from} ) {
                        my $alias = sprintf( $map->{relative}{alias}, $period );
                        $desired{$alias} = 1;
                    }
                }
            }
        }
    }
    my @updates = ();
    my %checks = map { $_ => 1 } keys(%desired),keys(%current);
    if( $managed ) {
        foreach my $alias (keys %checks) {
            if( exists $desired{$alias} && exists $current{$alias} ) {
                next;
            }
            my $action =  exists $desired{$alias} ? 'add' : 'remove';
            push @updates, { $action => { index => $index, alias => $alias} };

            # Do we skip an add or remove?
            next if exists $opt{skip} && $action eq $opt{skip};
            verbose({color=>'cyan'}, "$index: $action alias '$alias'");
        }
    }
    debug({color=>'magenta'}, "Aliases for $index : " . join(',', keys %desired) );
    if( @updates ) {
        eval {
            es_request('_aliases', { method => 'POST' }, { actions => \@updates });
            output({color=>'green'}, "Updates applied for $index : " . join(',', keys %desired) );
        };
        if( my $err = $@ ){
            output({color=>'red'}, " + Failed to set aliases for $index\n", $err);
        }
    }
}

__END__

=head1 SYNOPSIS

es-alias-manager.pl --local --config /etc/elasticsearch/aliases.yml

Options:

    --help              print help
    --manual            print full manual
    --config            Location of Config File, default /etc/elasticsearch/aliases.yml
    --skip              Action name to be skipped, 'add' or 'remove', default none

=from_other App::ElasticSearch::Utilities / ARGS / all

=from_other CLI::Helpers / ARGS / all

=head1 OPTIONS

=over 8

=item B<config>

Location of the config file, default is /etc/elasticsearch/aliases.yml

=item B<skip>

Optionally skip a phase of alias management, valid phases are: add, remove

=back

=head1 DESCRIPTION

This script assists in maintaining the aliases for relative or daily indexes across multiple datacenters.

Use with cron:

    22 4 * * * es-alias-manager.pl --local --config /etc/elasticsearch/aliases.yml

This will allow you to split your cluster between datacenters (or whatever) and alias the split clusters
to a homogenous index that standard LogStash/Kibana interfaces will understand.

If I create the following in /etc/elasticsearch/aliases.yml

    ---
    logstash:
      pattern: \*-logstash-{{DATE}}
      daily: logstash-{{DATE}}
      relative:
        alias: logstash-{{PERIOD}}
        periods:
          today:
            from:
              days: 0
            to:
              days: 0
          lastweek:
            from:
              days: 14
            to:
              days: 7

Assuming today is the 2013.07.18 and I have 3 datacenters (IAD, NYC, AMS) with the following indices:

    iad-logstash-2013.07.17
    iad-logstash-2013.07.18
    nyc-logstash-2013.07.17
    nyc-logstash-2013.07.18
    ams-logstash-2013.07.17
    ams-logstash-2013.07.18

The following aliases would be created

    logstash-2013.07.17
        |- iad-logstash-2013.07.17
        |- nyc-logstash-2013.07.17
        `- ams-logstash-2013.07.17

    logstash-2013.07.18
        |- iad-logstash-2013.07.18
        |- nyc-logstash-2013.07.18
        `- ams-logstash-2013.07.18

    logstash-today
        |- iad-logstash-2013.07.18
        |- nyc-logstash-2013.07.18
        `- ams-logstash-2013.07.18

This lets you use index templates and the index.routing.allocation to isolate data by datacenter or another
parameter to certain nodes while allowing all the nodes to work together as cleanly as possible.  This also facilitates
the default expectations of Kibana to have a single index per day when you may need more.

=head2 PATTERN VARIABLES

Patterns are used to match an index to the aliases it should have.  A few symbols are expanded into
regular expressions.  Those patterns are:

    The '*' expands to match any number of any characters.
    The '?' expands to match any single character.
    {{DATE}} expands to match YYYY.MM.DD, YYYY-MM-DD, or YYYYMMDD

=head2 ALIAS VARIABLES

For daily indices, the following variables are available:

    {{DATE}} - Expands to YYYY.MM.DD for the current day of the current index

For relative period indices, the following variable is B<required>.

    {{PERIOD}} - Name of the period

=cut
