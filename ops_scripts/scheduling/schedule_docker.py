#!/usr/bin/python

__author__ = 'nbyrne'

# Architecture 2.5 Scheduling Script
# Run this in the same folder as your generated ini files, and it will seed machines and start workflows for you

import glob
import os
import shlex
import shutil
import subprocess
import sys

# Constants


WORKFLOWNAME="Workflow_Bundle_DEWrapperWorkflow_"
SSHKEY_LOCATION = "/home/ubuntu/.ssh/wei-dkfz.pem"
GNOSKEY_LOCATION = "/home/ubuntu/.ssh/gnos.pem"



# Turn on to enable debugging
DEBUG=False
IP_REGEX = "\b((?:[0-9]{1,3}\.){3}[0-9]{1,3})\b"

CRONTAB = ""
CRONTAB += "#SCHEDULER: check for a runner script and execute it\n"
CRONTAB += "* * * * * [ -e /home/ubuntu/ini/runner.sh ] && bash /home/ubuntu/ini/runner.sh 2>&1 > ~/.scheduler.txt && mv /home/ubuntu/ini/runner.sh /home/ubuntu/ini/runner.ran\n\n"

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


def GetIni(directory):
    """ Gets a list of ini files from the absolute path 'dir'
    Args:
        directory:  the absolute path to the ini files.
    Returns:
        A list of filenames matching the *.ini naming convention.
    """
    files = [ os.path.basename(f) for f in glob.glob(os.path.join(directory, '*.ini')) ]
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


def FeedMachines(ips, directory, ini_files, key=SSHKEY_LOCATION, gnosfile=GNOSKEY_LOCATION):
    """ Send an ini file to a machine and execute it. """
    with open(gnosfile) as f:
        gnoskey = f.read()
    for ip in ips:
        ini = ini_files.pop()
        print "Scheduling on %s ... " % (ip)
        print "\t1) Creating remote directory ... "
        out, err, errcode = RunCommand("ssh -i %s ubuntu@%s \"mkdir ini\"" % (key, ip))
        print "\t2) Copying ini file to %s ... " % (ip)
        out, err, errcode = RunCommand("scp -i %s %s/%s ubuntu@%s:~/ini" % (key, directory, ini, ip))
        if errcode:
            sys.exit()
        print "\t3) Creating custom launching components ... "
        # Custom workflow launcher (Needs to be heavily customized to your environment)
        with open("monitor.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write("watch -n 5 \"docker logs `cat ~/.workflow_container`\"\n")
        with open ("runner.sh", "w") as f:
            f.write("cd /mnt/home/seqware\n")
            f.write("\n# Select A DEWrapper Version\n")
            f.write("files=$(ls /workflows | sort --unique)\n")
            f.write("regex=\"Workflow_Bundle_DEWrapperWorkflow_(.*)_SeqWare_1.1.0\"\n")
            f.write("for f in $files; do\n")
            f.write("   if [[ $f =~ $regex ]]; then\n")
            f.write("       version=${BASH_REMATCH[1]}\n")
            f.write("   fi\n")
            f.write("done")
            f.write("\n# Launch the workflow\n")
            f.write("container=$(docker run -d -h master -it -v /var/run/docker.sock:/var/run/docker.sock "
                    "-v /datastore:/datastore -v /workflows:/workflows -v /home/ubuntu/ini/%s:/workflow.ini "
                    "-v /home/ubuntu/ini/gnostest.pem:/home/ubuntu/.ssh/gnos.pem "
                    "seqware/seqware_whitestar_pancancer:1.1.1 "
                    "bash -c \"seqware bundle launch "
                    "--dir /workflows/Workflow_Bundle_DEWrapperWorkflow_${version}_SeqWare_1.1.0 "
                    "--engine whitestar --no-metadata --ini /workflow.ini\")\n" % ini)
            f.write("\n# Write the container ID\n")
            f.write("echo \"$container\" > ~/.workflow_container")
        with open ('crontab', 'w') as f:
            f.write(CRONTAB)
        with open('gnostest.pem', 'w') as f:
            f.write(gnoskey)
        print "\t4)  Copying custom launcher components ... "
        for fl in ['monitor.sh', 'runner.sh', 'crontab', 'gnostest.pem']:
            out, err, errcode = RunCommand("scp -i %s %s/%s ubuntu@%s:~/ini" % (key, directory, fl, ip))
            if errcode:
                    sys.exit()
        print "\t5)  Remotely scheduling workflow ..."
        out, err, errcode = RunCommand("ssh -i %s ubuntu@%s \"crontab ini/crontab\"" % (key,ip))
        out, err, errcode = RunCommand("ssh -i %s ubuntu@%s \"cp ini/monitor.sh ~/monitor\"" % (key,ip))
        try:
            os.mkdir(ip)
        except:
            pass
        print "\t5) Moving local ini file into a host folder"
        shutil.move(ini, os.path.join(ip,ini))
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