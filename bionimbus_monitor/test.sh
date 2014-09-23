#!/bin/bash

cd /glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor
# first pass to reboot failed nodes
perl test.pl --verbose --glob-base /glusterfs/netapp/homes1/BOCONNOR/gitroot/bindle_1.2.2 --glob-target 'target-os-c*'
# run and setup sensu, hopefully will setup sensu on freshly rebooted nodes
perl test.pl --verbose --glob-base /glusterfs/netapp/homes1/BOCONNOR/gitroot/bindle_1.2.2 --glob-target 'target-os-c*' --setup-sensu

