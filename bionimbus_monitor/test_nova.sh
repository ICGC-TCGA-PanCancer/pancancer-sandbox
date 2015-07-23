#!/bin/bash

cd /glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor
# checks and turns off nodes it can't ssh to
perl test.pl --verbose --use-nova --glob-target 'fleet_master' --cluster-json /glusterfs/netapp/homes1/BOCONNOR/gitroot/decider/decider-bwa-pancancer/pdc_cluster.2.json --ssh-pem /glusterfs/netapp/homes1/BOCONNOR/.ssh/brian-pdc-3.pem --ssh-username BOCONNOR
