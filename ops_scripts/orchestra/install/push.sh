#!/bin/bash

# Installs the webservice onto a single worker node, and tests it for functionality
# Takes an ip address and a key file as arguments

# If you don't want to blast a whole subnet, you can use this to provision
# individual nodes

# Although Orchestra will work in any posix environment, this install script
# is Ubuntu only

keyfile="$2"
target="$1"

# Skip machines you can't ping

ping -c 3 $target
[[ $? -ne 0 ]] && exit 0

# Create ansible inventory file
echo "[ seqware_worker ]" > inventory
echo "${target} ansible_ssh_private_key_file=${keyfile}" >> inventory

# Begin Remote Install as Background Process

ansible-playbook -i inventory site.yml

# Remote Test

curl $target:9009/healthy
