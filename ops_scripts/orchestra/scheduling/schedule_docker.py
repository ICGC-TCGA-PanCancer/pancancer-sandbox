#!/usr/bin/python

__author__ = 'nbyrne'

# Architecture 2.5 Scheduling Script
# Integrates with orchestra for automatic scheduling, and failure detection
# You need to modify the Configuration section to set file locations, and select a workflow

# The scheduler will install the GNOS pem file, the ini file, and a cronjob to launch the workflow.
# Log into the worker, and view the contents of /home/ubuntu/ini to see worker side configuration.
# Log into the worker and execute "bash monitor" in the ubuntu home folder to see the workflow in realtime

# Configuration
SSHKEY_LOCATION = "/home/ubuntu/.ssh/wei-dkfz.pem"
GNOSKEY_LOCATION = "/home/ubuntu/.ssh/gnos.pem"
INIFILE_LOCATION = "/home/ubuntu/central-decider-client/ini"

# Workflow Selection, uncomment the workflow you want to run
# WORKFLOW_REGEX = "(Workflow_Bundle_DEWrapperWorkflow_.*_SeqWare.*)"
# WORKFLOW_REGEX = "(Workflow_Bundle_BWA_.*_SeqWare.*)"
WORKFLOW_REGEX = "(Workflow_Bundle_HelloWorld_.*_SeqWare.*)"
# WORKFLOW_REGEX = "(Workflow_Bundle_SangerPancancerCgpCnIndelSnvStr_.*_SeqWare.*)"

# Import Modules
import glob
import logging
import os
import shlex
import shutil
import subprocess
import sys
import urllib2

# Turn on to enable debugging
DEBUG = False
IP_REGEX = "\b((?:[0-9]{1,3}\.){3}[0-9]{1,3})\b"
LOGFILE = "scheduler.log"

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

def DoubleSchedulingCheck(ini):
    """ Avoid double scheduling samples. """
    # Check for redundant scheduling - VERY HACKY
    all_files = []
    for root, dirs, files in os.walk("."):
        for name in files:
            all_files.append(name)
    if all_files.count(ini) > 1:
        print "SKIPPING: %s has already been scheduled." % ini
        print "\t(Move this file out of it's ip-address folder to schedule it again.)"
        return True
    return False

def GetIni(directory):
    """ Gets a list of ini files from the absolute path 'dir'
    Args:
        directory:  the absolute path to the ini files.
    Returns:
        A list of filenames matching the *.ini naming convention.
    """
    mypath =  os.getcwd()
    os.chdir(directory)
    files = [os.path.join(os.getcwd(), os.path.basename(f)) for f in glob.glob('*.ini')]
    os.chdir(mypath)
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

def CreateSchedulingContent(ip, directory, ini, gnosfile=GNOSKEY_LOCATION):
    """ Creates the files necessary for scheduling workflows.
    Arguments:
        directory:  The folder to create the content in
        ini:        The ini file to to schedule
        gnosfile:   The location of the gnos key file to use
    Returns:
        None
    """
    # Create the scheduling folder
    try:
        os.mkdir(directory)
    except OSError as e:
        pass

    # Read the gnos key into ram
    with open(gnosfile) as f:
        gnoskey = f.read()

    # Prepare monitoring Script
    content = """
#!/bin/bash
while true; do
    docker logs `cat /datastore/.worker/lastrun.cid`
    sleep 5
done
"""
    WriteFile(os.path.join(directory, "monitor.sh"), content)
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
# Copy the monitor script to the home folder for easy access
cp ~/ini/monitor.sh ~/monitor
"""
    WriteFile(os.path.join(directory, "runner.sh"), content)
    WriteFile(os.path.join(directory, "gnostest.pem"), gnoskey)

def FeedMachines(ips, ini_files, key=SSHKEY_LOCATION):
    """ Send an ini file to a machine and execute it. """

    if len(ini_files) == 0:
        print "There are no more ini files available for scheduling."
        sys.exit(1)

    print ini_files

    for ip in ips:
        # Check if the Machine is Busy
        if MachineBusy(ip):
            logging.warn("Machine %s reports being busy with a workflow." % ip)
            print "WARNING: Machine %s reports being busy with a workflow." % ip
            continue

        # Select a workflow, avoid doublescheduling things
        ini = ini_files.pop()
        while DoubleSchedulingCheck(ini):
            logging.warn("Scheduling conflict: %s was previously scheduled." % ini)
            ini = ini_files.pop()

        # Create the scheduling content for this machine
        schedulingfolder="/home/ubuntu/scheduling"
        CreateSchedulingContent(schedulingfolder, ini)

        # Create a single host ansible inventory file
        content = "%s ansible_ssh_private_key_file%s\n" % (ip, key)
        WriteFile(os.path.join(schedulingfolder, "inventory", content))

        # Call ansible to execute the install
        print "Scheduling %s on %s" % (ini, ip)
        logging.info("Scheduling %s on %s" % (ini, ip))
        shutil.copy(ini, os.path.join(schedulingfolder, ini))
        shutil.copy(schedulingfolder, "schedule.yml")
        mypath =  os.getcwd()
        os.chdir(schedulingfolder)
        our, err, errcode = RunCommand("ansible-playbook -i inventory schedule.yml ")
        os.chdir(mypath)
        if errcode:
            logging.error("Unable to schedule %s to %s." % (ini, ip))
            print "ERROR: scheduling %s to %s" % (ini, ip)
            continue
        shutil.move(ini, os.path.join(ip, ini))
        logging.info("Success scheduling %s to %s." % (ini, ip))
        print "SUCCESS: scheduling %s to %s" % (ini, ip)

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print "Usage:"
        print "python schedule 192.168.3.1,192.168.2.2"
        print ""
        sys.exit(1)

    # Schedule the ini files
    setup_logging(LOGFILE)
    logging.info("Scheduler was called with argument: %s" % (sys.argv[1]))
    ini_files = GetIni(INIFILE_LOCATION)
    FeedMachines(sys.argv[1].split(","), ini_files)
