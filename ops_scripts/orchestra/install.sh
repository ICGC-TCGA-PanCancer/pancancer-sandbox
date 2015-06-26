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
        echo "Create your orchestra subnet file."
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
sudo chmod +x orchestra.py

echo "Installing remote webservice on all nodes ..."
if [ -z $1 ]; then
    echo "    No inventory file specified- using auto discovery mode."
    echo "    This will be very time consuming.  Expect a long delay while this completes."
    subnet=`cat ~/.orchestra_subnet`
    python install/subnet-install.py $subnet $keyfile
    bash install/push.sh
else
    cd install
    bash install/push.sh
fi

echo "All done."
