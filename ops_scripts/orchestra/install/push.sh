#!/bin/bash

# Installs the webservice onto a single worker node, and tests it for functionality
# Takes an ip address and a key file as arguments

# If you don't want to blast a whole subnet, you can use this to provision
# individual nodes

# Although Orchestra will work in any posix environment, this install script
# is Ubuntu only

# Begin Remote Install as Background Process

ansible-playbook -i $1 site.yml

# Make the local cache file unique
cat ~/.orchestra_cache | sort --unique > ~/.orchestra_cache_new
mv ~/.orchestra_cache_new ~/.orchestra_cache