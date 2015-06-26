#!/usr/bin/python

__author__ = 'nbyrne'

# Architecture 2.5 Scheduling Script
# Run this in the same folder as your generated ini files, and it will seed machines and start workflows for you
# Totally customized for running DEWRAPPER

import glob
import os
import shlex
import shutil
import subprocess
import sys
import urllib2

# Configuration
SSHKEY_LOCATION = "/home/ubuntu/.ssh/wei-dkfz.pem"
GNOSKEY_LOCATION = "/home/ubuntu/.ssh/gnos.pem"
WORKFLOW_REGEX = "(Workflow_Bundle_DEWrapperWorkflow_.*_SeqWare.*)"

# Turn on to enable debugging
DEBUG = False

# Customizable crontab
CRONTAB = ""
CRONTAB += "#SCHEDULER: check for a runner script and execute it\n"
CRONTAB += "* * * * * [ -e /home/ubuntu/ini/runner.sh ] && mv /home/ubuntu/ini/runner.sh /home/ubuntu/ini/runner.ran && bash /home/ubuntu/ini/runner.ran 2>&1 > ~/.scheduler.txt \n\n"

IP_REGEX = "\b((?:[0-9]{1,3}\.){3}[0-9]{1,3})\b"


def MachineBusy(ip):
    """ Uses the orchestra webservice to determine if a workflow is already running. """
    try:
        data = urllib2.urlopen("http://%s:9009/busy" % ip, timeout=5).read().strip()
    except:
        print "The orchestra webservice is not responding on this machine."
        return False
    if data == "TRUE":
        print "This machine is already processing a workflow."
        return True
    return False


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


def DoubleSchedulingCheck(ini):
    """ Avoid double scheduling samples. """
    # Check for redundant scheduling - VERY HACKY
    all_files = []
    for root, dirs, files in os.walk("."):
        for name in files:
            all_files.append(name)
    if all_files.count(ini) > 1:
        print "Duplicate scheduling is being avoided for %s" % ini
        return True
    return False


def GetIni(directory):
    """ Gets a list of ini files from the absolute path 'dir'
    Args:
        directory:  the absolute path to the ini files.
    Returns:
        A list of filenames matching the *.ini naming convention.
    """
    files = [os.path.basename(f) for f in glob.glob(os.path.join(directory, '*.ini'))]
    return files


def GetMachines():
    """ Interacts with the nova command line to get a list of machines to schedule on. """
    out, err, errcode = RunCommand("nova list")
    if errcode !=0:
        sys.exit(errcode)
    data = out.split('\n')
    machines = []
    for line in data:
        match = re.search(IP_REGEX, line)
        if match is not None:
            machines.append(match.group(1))
    return machines


def WriteFile(filename, body):
    """ Writes out a string to the specified filename.
    Arguments:
        filename:   A string representing the name of the file to write.
        body:       A string to be written as the contents of the file.
    Returns:
        None
    """
    with open(filename, "w") as f:
            f.write(body+"\n")


def CreateSchedulingContent(ini, gnosfile=GNOSKEY_LOCATION):
    """ Creates the files necessary for scheduling workflows.
    Arguments:
        ini:        The ini file to to schedule
        gnosfile:   The location of the gnos key file to use
    Returns:
        None
    """
    # Read the gnos key into ram
    with open(gnosfile) as f:
        gnoskey = f.read()

    # Custom workflow launcher (Needs to be heavily customized to your environment)
    # Prepare monitoring Script
    content = """
#!/bin/bash
while true; do
    docker logs `cat /datastore/.worker/lastrun.cid`
    sleep 5
done
"""
    WriteFile("monitor.sh", content)
    # Prepare runner Script
    content = """
#!/bin/bash
cd /home/ubuntu
# Get the latest version of the workflow
files=$(ls /workflows | sort)
"""
    content += "regex=\"%s\"" % WORKFLOW_REGEX
    content += """
for f in $files; do
   if [[ -f /workflows/$f ]]; then
       continue
   fi
   if [[ $f =~ $regex ]]; then
       workflow=${BASH_REMATCH[1]}
   fi
   done
# Launch the workflow
"""
    content += (
        "[[ ! -e /datastore/.worker ]] && mkdir /datastore/.worker\n"
        "[[ ! -e /datastore/.worker/success.cid ]] && touch /datastore/.worker/success.cid\n"
        "[[ -e /datastore/.worker/lastrun.cid ]] && rm /datastore/.worker/lastrun.cid\n"
        "docker run --cidfile=\"/datastore/.worker/lastrun.cid\" -d -h master -it "
        "-v /var/run/docker.sock:/var/run/docker.sock "
        "-v /datastore:/datastore "
        "-v /workflows:/workflows "
        "-v /home/ubuntu/ini/%s:/workflow.ini "
        "-v /home/ubuntu/ini/gnostest.pem:/home/ubuntu/.ssh/gnos.pem "
        "seqware/seqware_whitestar_pancancer:1.1.1 "
        "bash -c \"seqware bundle launch "
        "--dir /workflows/${workflow} "
        "--engine whitestar --no-metadata --ini /workflow.ini && "
        "cat /datastore/.worker/lastrun.cid >> /datastore/.worker/success.cid"
        "\"\n" % ini
    )
    content += """
# Copy the monitor to the home folder for easy access
cp ~/ini/monitor.sh ~/monitor
"""
    WriteFile("runner.sh", content)
    WriteFile("crontab", CRONTAB)
    WriteFile("gnostest.pem", gnoskey)


def FeedMachines(ips, directory, ini_files, key=SSHKEY_LOCATION):
    """ Send an ini file to a machine and execute it. """
    for ip in ips:
        # Check if the Machine is Busy
        if MachineBusy(ip):
            continue

        # Select a workflow, avoid doublescheduling things
        ini = ini_files.pop()
        while DoubleSchedulingCheck(ini):
            ini = ini_files.pop()

        # Start Scheduling Pipeline
        print "Scheduling on %s ... " % ip
        print "\t1) Creating remote directory ... "
        out, err, errcode = RunCommand("ssh -i %s ubuntu@%s \"mkdir ini\"" % (key, ip))
        print "\t2) Copying ini file to %s ... " % ip
        out, err, errcode = RunCommand("scp -i %s %s/%s ubuntu@%s:~/ini" % (key, directory, ini, ip))
        if errcode:
            sys.exit()
        print "\t3) Creating custom launching components ... "
        # Custom workflow launcher scripts
        CreateSchedulingContent(ini)
        print "\t4) Copying custom launcher components ... "
        for fl in ['monitor.sh', 'runner.sh', 'crontab', 'gnostest.pem']:
            out, err, errcode = RunCommand("scp -i %s %s/%s ubuntu@%s:~/ini" % (key, directory, fl, ip))
            if errcode:
                sys.exit()
        print "\t5) Remotely scheduling workflow ..."
        out, err, errcode = RunCommand("ssh -i %s ubuntu@%s \"crontab ini/crontab\"" % (key, ip))
        try:
            os.mkdir(ip)
        except:
            pass
        print "\t5) Moving local ini file into a host folder"
        shutil.move(ini, os.path.join(ip, ini))
        print "\t6) All steps are complete"


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print "Usage:"
        print "python schedule 192.168.3.1,192.168.2.2"
        print ""
        sys.exit(1)

    # launch a single workflow
    ini_files = GetIni(".")
    FeedMachines(sys.argv[1].split(","), ".", ini_files)