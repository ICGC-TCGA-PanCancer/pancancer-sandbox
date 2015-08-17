#!/bin/bash

# Schedules jobs on any current lazy workers
# Workers with failed workflows will not be schduled to

/bin/orchestra lazy | grep -v RESPONDING > .lazy_hosts

for x in `cat .lazy_hosts`; do
	echo "Scheduling on $x" >> auto-scheduler.log
	orchestra schedule $x >> auto-scheduler.log
done

