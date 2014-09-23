# README

## Overview

### Failed Node Detection

A simple script to identify failed nodes, restart them, and, for now, turn off SGE on them after reboot.  Keep in mind this is for BioNimbus but could be helpful for other sites too. A future version could restart the node such that it is used for computation again however testing shows these nodes do not come back online.  When I reboot and restart SGE I find the same nodes very quickly return to a bad state e.g. filesystem locked up.  So for the time being this script essentially blacklists failed nodes.

The checks performed:

* checks to see if we can ssh to the host, if not call nova to force reboot
* for hosts that can ssh, check to make sure their hostname matches. If not this is a sign the host has been rebooted and, therefore, should be blacklisted since it's likely to fail again.

### Monitoring

## Installation

### Launcher Host

### Clients

## Running Interactively

## Running as a Cron

## Future Work

This tool needs to be integrated into a more generic monitoring system for PanCancer. Probably better to remove the Perl script and implement as "handlers" from Sensu.  Also, would be great to have this work with an AWS host as the Sensu host, allowing us to share the status on PanCancer.info.  Finally, would be great to include monitors for workflow status as well.
