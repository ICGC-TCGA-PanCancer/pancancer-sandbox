#!/usr/bin/python

__author__ = 'nbyrne'

# Installs the mangement_interface on all nodes in a subnet

import netaddr
import os
import sys

def main(subnet, keyfile):
    for ip in netaddr.IPNetwork(subnet):
        os.system("bash install/push.sh %s %s" % (ip, keyfile))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: python subnet-install.py [cidr] [ssh keyfile]\n\n"
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
