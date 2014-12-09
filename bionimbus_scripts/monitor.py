#!/usr/bin/python

# Monitors the hosts running in bionimbus

import glob
import re
import shlex
import subprocess
import sys
import os

PEM="/glusterfs/netapp/homes1/BOCONNOR/.ssh/brian-pdc-3.pem"

def RunCommand(string, needshell=False):
    """
        Simple function that executes a system call.
        Args:
            string  The command to run.
        Returns:
            out         Stdout contents.
            err         Stderr contents.
            code        Exit code returned by the call.
    """
    p = subprocess.Popen(shlex.split(string), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=needshell)
    out, err = p.communicate()
    p.wait()
    if p.returncode != 0:
        print err
        sys.exit()
    return out, err, p.returncode

class Host(object):
    def __init__(self,ip, hostname):
        self.ip=ip
        self.hostname=hostname
        self.output = None
    def RemoteCmd(self, cmd):
        out, err, code = RunCommand("ssh -i %s BOCONNOR@%s \"%s\"" % (PEM, self.ip, cmd))
        self.output = out

def main():
    # create hosts
    hosts = []
    out, err, code = RunCommand("nova list")
    novalist = out.split('\n')
    for line in novalist:
        if line == "+--------------------------------------+--------------------------+--------+------------+-------------+-------------------+":
            continue
        data = line.split('|')
        if len(data) > 6:
            if 'b_sanger' in data[2]:
                host = data[2].strip()
                match = re.search(r'(\d+.\d+.\d+.\d+)',data[6])
                if match:
                    hosts.append(Host(match.group(1), host))
    # monitor hosts
    while True:
        print "Checking hosts ..."
        for host in hosts:
            host.RemoteCmd("oozie jobs -oozie http://master:11000/oozie | grep RUNNING | wc -l")
            if int(host.output) > 1:
                print "ERROR: host %s is running more than 1 oozie job." % (host.hostname)
            elif int(host.output) < 1:
                print "ERROR: host %s is not running any jobs." % (host.hostname)
            else:
                print "Host %s is fine." % (host.hostname)
        os.system("sleep 30")

if __name__ == '__main__':
    main()
    
