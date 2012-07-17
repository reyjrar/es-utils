# ABSTRACT: Utilities for Monitoring ElasticSearch
package es::utils;

=head1 SYNOPSIS

This a set of utilities to make monitoring ElasticSearch clusters much simpler.

Included is:

    scripts/es-status.pl - Command line utility for ES Metrics
    scripts/es-metrics-to-graphite.pl - Send ES Metrics to Graphite or Cacti
    scripts/es-nagios-check.pl - Monitor ES remotely or via NRPE with this script

The es::utils module simply serves as a wrapper around the scripts for packaging and
distribution.

=head1 INSTALL

To install the utilities, simply:

    wget --no-check-certificate https://github.com/reyjrar/es-utils/tarball/master -O es-utils.tar.gz

    tar -zxvf es-utils.tar.gz

    cd reyjrar-es-utils-*

    perl Makefile.PL

    make

    make install

This will take care of ensuring all the dependencies are satisfied and will install the scripts into the same
directory as your Perl executable.

=head2 USAGE

The tools are all wrapped in their own documentation, please see:

    es-status.pl --help
    es-metric-to-graphite.pl --help
    es-nagios-check.pl --help

For individual options and capabilities


=cut


1;
