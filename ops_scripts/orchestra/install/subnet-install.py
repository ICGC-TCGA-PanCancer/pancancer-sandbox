#!/usr/bin/python

__author__ = 'nbyrne'

# Creates an ansible inventory file for installing the Orchestra webservice
# It writes the entire subnet to the file, so that an install is tried on all IP's
# Failures are expected

import netaddr
import sys

def main(subnet, keyfile):
    with open("inventory", "w") as f:
        f.write("[ seqware_worker ]\n")
        for ip in netaddr.IPNetwork(subnet):
            f.write(str(ip)+" ansible_ssh_private_key_file=%s\n" % sys.argv[2])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: python subnet-install.py [cidr] [ssh keyfile]\n\n"
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
