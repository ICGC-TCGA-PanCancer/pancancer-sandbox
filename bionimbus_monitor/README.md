# README

## Overview

### Failed Node Detection

A simple script to identify failed nodes, restart them, and, for now, turn off SGE on them after reboot.  Keep in mind this is for BioNimbus but could be helpful for other sites too. A future version could restart the node such that it is used for computation again however testing shows these nodes do not come back online.  When I reboot and restart SGE I find the same nodes very quickly return to a bad state e.g. filesystem locked up.  So for the time being this script essentially blacklists failed nodes.

The checks performed:

* checks to see if we can ssh to the host, if not call nova to force reboot
* for hosts that can ssh, check to make sure their hostname matches. If not this is a sign the host has been rebooted and, therefore, should be blacklisted since it's likely to fail again.

### Monitoring

This tool can optionally install sensu clients and configure them to point to a sensu host.  This lets us use a nice monitoring GUI to track the state of the clusters.  You'll need to setup the sensu server to receive these messages.

## Installation

You'll want to setup the Perl script to run on your launcher host, typically in a cron job.

If this is a launcher host I'm assuming you have Vagrant properly setup and you've launched some clusters using Bindle.

For the Sensu server on this launcher host, follow the directions to set this up from here: http://sensuapp.org/

### Sensu Checks

You need to include checks on the sensu server side, these will be run on the various worker nodes.  Here's my sensu config:

    {
      "rabbitmq": {
        "host": "localhost",
        "port": 5672,
        "user": "sensu",
        "password": "REDACTED",
        "vhost": "/sensu"
      },
      "api": {
        "host": "localhost",
        "port": 4567,
        "user": "sensu",
        "password": "REDACTED",
        "bind": "0.0.0.0"
      },
      "redis": {
        "host": "localhost",
        "port": 6379
      },
      "handlers": {
        "default": {
          "type": "set",
          "handlers": [
            "stdout"
          ]
        },
        "stdout": {
          "type": "pipe",
          "command": "cat"
        }
      },
      "checks": {
        "available_check": {
          "handlers": ["default"],
          "command": "echo -n OK",
          "interval": 60,
          "subscribers": [ "tests" ]
        },
        "netapp_check": {
          "command": "unzip -c /glusterfs/netapp/homes1/BOCONNOR/provisioned-bundles/Workflow_Bundle_BWA_2.6.0_SeqWare_1.0.15/Workflow_Bundle_BWA/2.6.0/bin/jre1.7.0_51/lib/rt.jar > /dev/null",
          "subscribers": [ "chicago-tests" ],
          "interval": 60
        },
        "gluster_check": {
          "command": "ls /mnt/glusterfs/data/ICGC1/scratch/seqware_results/ && ls /mnt/glusterfs/data/ICGC2/seqware_results_icgc/completed/ && ls /mnt/glusterfs/data/ICGC3/seqware_results_icgc/completed/",
          "subscribers": [ "chicago-tests" ],
          "interval": 60
        },
        "gridengine_master_check": {
          "command": "ps aux | grep sge_qmaster | wc -l | perl -e 'while(<STDIN>) { chomp; if ($_ eq '2') {exit 1;} exit 0; }'",
          "subscribers": [ "master-tests" ],
          "interval": 60
        },
        "gridengine_worker_check": {
          "command": "ps aux | grep sge_execd | wc -l | perl -e 'while(<STDIN>) { chomp; if ($_ eq '2') {exit 1;} exit 0; }'",
          "subscribers": [ "worker-tests" ],
          "interval": 60
        }
      },
      "client": {
        "name": "chicago-launcher",
        "address": "127.0.0.1",
        "subscriptions": [
          "tests"
        ]
      }
    }

### Port Forwarding

Make sure you setup Uchiwa during the Sensu setup.

Use port forwarding to see the Uchiwa console for Sensu:

    ssh -L 8080:localhost:8080 -L 4567:localhost:4567 -i ~/.ssh/key.pem user@host.opensciencedatacloud.org

## Running Interactively

It's just a perl script so you can run on the command line as the user that launched clusters with Bindle:

    USAGE: perl test.pl [--test] [--verbose] [--setup-sensu] [--glob-base <path to directory that contains bindle dirs>] [--glob-target <target-*>]

For example:

    #!/bin/bash
    
    cd /glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor
    perl test.pl --verbose --glob-base /glusterfs/netapp/homes1/BOCONNOR/gitroot/bindle_1.2.2 --glob-target 'target-os-c*'

This will process the clusters in each of the target-os-c* directories.  It will reboot nodes that can't ssh to and blacklist those (by turning off SGE) that have previously been rebooted.

You can also run with the --setup-sensu tag that will install the sensu client and point it at the current (hard coded) sensu server.

## Running as a Cron

I made a simple cron shell script that causes the monitor to be ran twice, the first to reboot stuck hosts and the second to make sure sensu is setup on those hosts.

    #!/bin/bash
    
    cd /glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor
    # first pass to reboot failed nodes
    perl test.pl --verbose --glob-base /glusterfs/netapp/homes1/BOCONNOR/gitroot/bindle_1.2.2 --glob-target 'target-os-c*'
    # run and setup sensu, hopefully will setup sensu on freshly rebooted nodes
    perl test.pl --verbose --glob-base /glusterfs/netapp/homes1/BOCONNOR/gitroot/bindle_1.2.2 --glob-target 'target-os-c*' --setup-sensu


## Future Work

This tool needs to be integrated into a more generic monitoring system for PanCancer. Probably better to remove the Perl script and implement as "handlers" from Sensu.  Also, would be great to have this work with an AWS host as the Sensu host, allowing us to share the status on PanCancer.info.  Finally, would be great to include monitors for workflow status as well which would properly restart failed workflows.
