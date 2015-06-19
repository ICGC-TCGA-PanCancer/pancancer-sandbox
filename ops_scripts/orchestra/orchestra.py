#!/usr/bin/python

__author__ = 'nbyrne'

# Orchestra CLI

import netaddr
import os
import sys
import urllib2


# CONSTANTS
CACHEFILE = os.path.join(os.getenv("HOME"), ".orchestra_cache")
SUBNET = os.path.join(os.getenv("HOME"), ".orchestra_subnet")


def parsefail():
    """Simple help message when parsing the command arguments fails."""
    print "Try: orchestra help\n"
    sys.exit(1)


def readsubnetfile():
    """ Simple helper function that returns the contents of the subnet file. """
    if not os.path.exists(SUBNET):
        print "No subnet file found.  You'll need to create one.\n"
        sys.exit(1)
    with open(SUBNET) as f:
        data = f.read()
    return data


def RunCommand(cmd):
    """ Execute a system call safely, and return output.
    Args:
        cmd:        A string containing the command to run.
    Returns:
        out:        A string containing stdout.
        err:        A string containing stderr.
        errcode:    The error code returned by the system call.
    """
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    errcode = p.returncode
    if DEBUG:
        print cmd
        print out
        print err
        print errcode
    return out, err, errcode


def main():
    SUBNET = readsubnetfile()
    
    # Handle the list command - find all nodes with ochestra installed
    if sys.argv[1] == "list":
        with open(CACHEFILE, "w") as f:
            for ip in netaddr.IPNetwork(SUBNET):
                try:
                    data = urllib2.urlopen("http://%s:9009/healthy" % ip, timeout=2).read().strip()
                except:
                    data = "FALSE"
                if data == "TRUE":
                    print ip
                    f.write(str(ip)+"\n")
    
    # Handle the busy, and lazy commands - use cached data to find nodes
    if sys.argv[1] == "busy" or sys.argv[1] == "lazy":
        if not os.path.exists(CACHEFILE):
            print "No cache file found: Run 'orchestra list' to create one.\n"
            sys.exit(1)
        with open(CACHEFILE) as f:
            targets = f.read().strip().split("\n")
        for ip in targets:
            try:
                data = urllib2.urlopen("http://%s:9009/busy" % ip, timeout=5).read().strip()
            except:
                data = "FALSE"
            if data == "TRUE" and sys.argv[1] == "busy":
                print ip
            if data == "FALSE" and sys.argv[1] == "lazy":
                print ip
    
    # List the workflows on a particular machine
    if sys.argv[1] == "workflows":
        ip = sys.argv[2]
        try:
            data = urllib2.urlopen("http://%s:9009/workflows" % ip, timeout=5).read().strip()
        except:
            data = "FALSE"
        print data
    
    # Integrate with the scheduler to launch an ini file
    if sys.argv[1] == "schedule":
        ip = sys.argv[2]
        ini = sys.argv[3]
        print "NOT IMPLEMENTED YET"
    sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == "help":
            print "Valid Commands:"
            print "\torchestra list -- retrieve a list of all servers on this subnet running orchestra."
            print "\torchestra busy -- retrieve a list of all servers on this subnet running workflows."
            print "\torchestra lazy -- retrieve a list of all servers on this subnet not running workflows."
            print "\torchestra workflows [ip address] -- retrieve a list of all workflows on this machine."
            print "\torchestra schedule [ip address] [ini file] -- send an ini file to a machine and run it."
            print ""
            sys.exit(0)
        if sys.argv[1] == "busy" or sys.argv[1] == "lazy" or sys.argv[1] == "list":
            main()
    if sys.argv[1] == "workflows" and len(sys.argv) == 3:
        main()
    if sys.argv[1] == "schedule" and len(sys.argv) == 4:
        main()
    parsefail()
