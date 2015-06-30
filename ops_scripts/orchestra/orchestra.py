#!/usr/bin/python

__author__ = 'nbyrne'

# Orchestra CLI

import logging
import netaddr
import os
import shlex
import subprocess
import sys
import urllib2

# CONSTANTS
CACHEFILE = os.path.join(os.getenv("HOME"), ".orchestra_cache")
SUBNET = os.path.join(os.getenv("HOME"), ".orchestra_subnet")
LOGFILE = "/home/ubuntu/.orchestra.log"
DEBUG = False
TIMEOUT = 10

def setup_logging(filename, level=logging.INFO):
    """ Logging Module Interface.
    Args:
        filename:   The filename to log to.
        level:      The logging level desired.
    Returns:
        None
    """
    logging.basicConfig(filename=filename,level=level)
    return None

def parsefail():
    """Simple help message when parsing the command arguments fails."""
    print "Try: orchestra help\n"
    sys.exit(1)

def RunCommand(cmd):
    """ Execute a system call safely, and return output.
    Args:
        cmd:        A string containing the command to run.
    Returns:
        out:        A string containing stdout.
        err:        A string containing stderr.
        errcode:    The error code returned by the system call.
    """
    logging.info("System call: %s" % cmd)
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    errcode = p.returncode
    logging.info("Return code: %s" % errcode)
    if errcode:
        logging.error(err)
    if DEBUG:
        print cmd
        print out
        print err
        print errcode
    return out, err, errcode

def HealthStatus(ip):
    """  Processes the list command.  """
    try:
        data = urllib2.urlopen("http://%s:9009/healthy" % ip, timeout=TIMEOUT).read().strip()
    except:
        data = "FALSE"
    if data == "TRUE":
        return True
    else:
        return False

def HostList():
    """ Processes the hosts command. """
    result = []
    notresponding = []
    busyhosts = WorkerStatus("busy")
    if not os.path.exists(CACHEFILE):
            print "No cache file found: Run 'orchestra list' to create one.\n"
            sys.exit(1)
    with open(CACHEFILE) as f:
        targets = f.read().strip().split("\n")
    for ip in targets:
        if not HealthStatus(ip):
            notresponding.append("%s\tIS NOT RESPONDING!" % ip)
            continue
        if ip in busyhosts:
            container = LastContainer(ip)
            if len(container.strip()) == 0:
                container = "Unknown CID"
            # Try to get the donor name
            try:
                donor = urllib2.urlopen("http://%s:9009/lastini" % ip, timeout=TIMEOUT).read().strip()
                if len(donor) == 0:
                    donor = "Unknown"
            except:
                donor = "Unknown"
            result.append("%s\tIni: %s\tCID: %s" % (ip, donor, container))
        else:
            result.append("%s\tCurrently idle." % ip)
    sorted(result)
    return result + notresponding

def WorkerStatus(cmd):
    """ Processes the busy and lazy commands. """
    result = []
    notresponding = []
    if not os.path.exists(CACHEFILE):
            print "No cache file found: Run 'orchestra list' to create one.\n"
            sys.exit(1)
    with open(CACHEFILE) as f:
        targets = f.read().strip().split("\n")
    for ip in targets:
        if not HealthStatus(ip):
            notresponding.append("%s\tIS NOT RESPONDING!" % ip)
            continue
        try:
            data = urllib2.urlopen("http://%s:9009/busy" % ip, timeout=TIMEOUT).read().strip()
        except:
            # Workers who don't respond to the request won't be listed
            data = ""
        if data == "TRUE" and cmd == "busy":
            result.append(ip)
        if data == "FALSE" and cmd == "lazy":
            result.append(ip)
    sorted(result)
    return result + notresponding

def ListWorkflows(ip):
    """ Processes the workflows command """
    try:
        data = urllib2.urlopen("http://%s:9009/workflows" % ip, timeout=TIMEOUT).read().strip()
    except:
        data = ""
    print data

def LastContainer(ip):
    """ Processes a helper command to view the last run container id """
    try:
        data = urllib2.urlopen("http://%s:9009/lastcontainer" % ip, timeout=TIMEOUT).read().strip()
    except:
        data = "CID FILE NOT FOUND!"
    if len(data.strip()) == 0:
        data = "CID FILE NOT FOUND!"
    return data

def SuccessContainers(ip):
    """ Processes a helper function to list sucessful container id's """
    try:
        data = urllib2.urlopen("http://%s:9009/success" % ip, timeout=TIMEOUT).read().strip()
    except:
        data = "No successful workflow runs yet!"
    if len(data.strip()) == 0:
        data = "No successful workflow runs yet!"
    return data.split('\n')

def HasFailed(ip):
    """ Processes the failure command """
    success = SuccessContainers(ip)
    last = LastContainer(ip)
    if last in success:
        return True
    return False

def Schedule(ip):
    """ Processes the schedule command """
    # Do not schedule to nodes not running the webservice
    if not HealthStatus(sys.argv[2]):
        print "The webservice is not responding on the machine at: %s" % ip
        sys.exit(1)

    # Do not schedule to busy machines
    data = WorkerStatus("busy")
    if ip in data:
        print "This machine is already running a workflow."
        sys.exit(1)

    if HasFailed(ip):
        print "This machine is not being scheduled to right now: LOG INTO THIS MACHINE!"
        print "\tTo check view the logs from the last run workflow: bash ~/monitor"
        print "\tTo schedule to this machine again: rm /datastore/.worker/lastrun.cid"
        print "\tTo re-run the last failed ls" \
              "workflow:  bash /home/ubuntu/ini/runner.ran"
        print ""
    else:
        # chdir to the scheduler folder, and call the scheduler symlink
        mypath = os.getcwd()
        os.chdir("/bin/orchestra_scheduler")
        out, err, errcode = RunCommand("python schedule_docker.py %s" % ip)
        if errcode:
            print out, err
            sys.exit(1)
        os.chdir(mypath)
        print out, err

def main():
    
    # Handle the busy, and lazy commands - use cached data to find nodes
    if sys.argv[1] == "busy" or sys.argv[1] == "lazy":
        print "\n".join(WorkerStatus(sys.argv[1]))

    # Handle the hosts command
    if sys.argv[1] == "hosts":
        print "\n".join(HostList())

    # Check the webservice on a particular machine
    if sys.argv[1] == "check":
        if HealthStatus(sys.argv[2]):
            print "OK"
        else:
            print "FAILURE"

    # List the workflows on a particular machine
    if sys.argv[1] == "workflows":
        ListWorkflows(sys.argv[2])
    
    # Integrate with the scheduler to launch an ini file
    if sys.argv[1] == "schedule":
        ip = sys.argv[2]
        Schedule(ip)

    sys.exit(0)

def LogArguments():
    args = ",".join(sys.argv)
    logging.info("orchestra was called with arguments %s" % args)

if __name__ == '__main__':
    setup_logging(LOGFILE)
    LogArguments()
    if len(sys.argv) == 2:
        if sys.argv[1] == "help":
            print "Valid Commands:"
            print "\torchestra hosts -- retrieve a list of all hosts, and their current status"
            print "\torchestra busy -- retrieve a list of all servers on this subnet running workflows."
            print "\torchestra lazy -- retrieve a list of all servers on this subnet not running workflows."
            print "\torchestra workflows [ip address] -- retrieve a list of all workflows on this machine."
            print "\torchestra schedule [ip address] -- run the scheduler on the host specified."
            print "\torchestra check [ip address] -- confirms orchestra webservice is running remotely."
            print ""
            sys.exit(0)
        if sys.argv[1] == "busy" or sys.argv[1] == "lazy" or sys.argv[1] == "hosts":
            main()
    if len(sys.argv) > 1:
        if sys.argv[1] == "workflows" and len(sys.argv) == 3:
            main()
        if sys.argv[1] == "check" and len(sys.argv) == 3:
            main()
        if sys.argv[1] == "schedule" and len(sys.argv) == 3:
            main()
    parsefail()
