#!/bin/bash

set -e

# Modify this to point to the key file needed to access the nodes
keyfile="~/.ssh/wei-dkfz.pem"

if [ ! -f ~/.orchestra_subnet ]; then
    echo "Create your orchestra subnet file."
    exit 1
fi

subnet=`cat ~/.orchestra_subnet`
echo "Installing dependencies ..."
sudo apt-get update 2>&1 > install.log
sudo apt-get install -y software-properties-common 2>&1 >> install.log
sudo apt-add-repository ppa:ansible/ansible 2>&1 >> install.log
sudo apt-get update 2>&1 >> install.log
sudo apt-get install -y python-pip ansible 2>&1 >> install.log
sudo pip install netaddr 2>&1 >> install.log
echo "Installing orchestra CLI ..."
sudo sudo ln -sf `pwd`/orchestra.py /bin/orchestra
sudo chmod +x orchestra.py
echo "Installing remote webservice on all nodes ..."
python install/subnet-install.py $subnet $keyfile
echo "All done."
