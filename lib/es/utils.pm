# ABSTRACT: Utilities for Monitoring ElasticSearch
package es::utils;

=head1 NAME

es::utils - Utilities for monitoring ElasticSearch

=head1 SYNOPSIS

This a set of utilities to make monitoring ElasticSearch clusters much simpler.

Included is:

    scripts/es-status.pl - Command line utility for ES Metrics
    scripts/es-metrics-to-graphite.pl - Send ES Metrics to Graphite or Cacti
    scripts/es-nagios-check.pl - Monitor ES remotely or via NRPE with this script

The es::utils module simply serves as a wrapper around the scripts for packaging and
distribution.

=cut


1;
