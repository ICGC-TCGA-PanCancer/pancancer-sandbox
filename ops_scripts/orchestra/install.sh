#!/bin/bash

set -e

# Modify this to point to the key file needed to access the nodes
keyfile="~/.ssh/wei-dkfz.pem"

if [ ! -z $1 ]; then
    if [ ! -f $1 ]; then
        echo "Inventory file not found."
        exit 1
    fi
else
    if [ ! -f ~/.orchestra_subnet ]; then
        echo "For an automated install create your orchestra subnet file (vi ~/.orchestra_subnet)."
        exit 1
    fi
fi

echo "Installing dependencies ..."
sudo apt-get update 2>&1 > install.log
sudo apt-get install -y software-properties-common 2>&1 >> install.log
sudo apt-add-repository -y ppa:ansible/ansible 2>&1 >> install.log
sudo apt-get update 2>&1 >> install.log
sudo apt-get install -y python-pip ansible 2>&1 >> install.log
sudo pip install netaddr 2>&1 >> install.log

echo "Installing orchestra CLI ..."
sudo sudo ln -sf `pwd`/orchestra.py /bin/orchestra
sudo sudo ln -sf `pwd`/scheduling /bin/orchestra_scheduler
sudo chmod +x orchestra.py

echo "Installing remote webservice on all nodes ..."
if [ -z $1 ]; then
    echo "    No inventory file specified- using auto discovery mode."
    echo "    This will be very time consuming.  Expect some delay while this completes."
    subnet=`cat ~/.orchestra_subnet`
    python install/subnet-install.py $subnet $keyfile
    cp inventory install
    cd install
    bash push.sh inventory
else
    cp "$1" install
    cd install
    bash push.sh $1
fi

# Create the initial orchestra cache file based on successes


echo "All done."
