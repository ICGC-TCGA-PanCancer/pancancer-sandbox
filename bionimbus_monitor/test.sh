#!/bin/bash

cd /glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor
#  --setup-sensu 
perl test.pl --verbose --glob-base /glusterfs/netapp/homes1/BOCONNOR/gitroot/bindle_1.2.2 --glob-target 'target-os-c*'

