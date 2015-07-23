#!/usr/bin/python

# Installs a workflow if it isn't already on board
# Then schedules the job, and patches the crontab

import os
import re
import popen2
import shlex
import subprocess
import sys

WORKFLOW_SEARCH=r"/glusterfs/netapp/homes1/BOCONNOR/provisioned-bundles/Workflow_Bundle_SangerPancancerCgpCnIndelSnvStr_1.0.1_SeqWare_1.1.0-alpha.5"
WORKFLOW_INSTALL_DIR="/glusterfs/netapp/homes1/BOCONNOR/gitroot/SeqWare-CGP-SomaticCore_rc/extras"
WORKFLOW_INSTALL_COMMAND="seqware_1.1.0-alpha.5 bundle install-dir-only --dir ~/provisioned-bundles/Workflow_Bundle_SangerPancancerCgpCnIndelSnvStr_1.0.1_SeqWare_1.1.0-alpha.5"

SCHEDULE_WORKFLOW_COMMAND="seqware_1.1.0-alpha.5 workflow schedule --accession"
INI_FILE_PATH="--ini /glusterfs/netapp/homes1/BOCONNOR/workflow-dev/20141207_test_runs/"
INI_FILE_NAME="config.ini"
HOST_CLAUSE="--host master"

CRON_ON="*/5 * * * *  ~/crons/status_1.1.0-alpha.5.cron >> /tmp/status_1.1.0-alpha.5.cron.log"
CRON_OFF="#*/5 * * * *  ~/crons/status_1.1.0-alpha.5.cron >> /tmp/status_1.1.0-alpha.5.cron.log"

SWID_SEARCH_REGEX1=r"SWID:\s*\((\d+)\)"
SWID_SEARCH_REGEX2=r"SWID:\s*(\d+)"

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
    return out, err, p.returncode

def GetSwid(string):
    match = re.search(SWID_SEARCH_REGEX1,string)
    if match:
        return match.group(1)
    else:
        match = re.search(SWID_SEARCH_REGEX2,string)
        if match:
            return match.group(1)
        else:
            return None

def isWorkflowInstalled(search_string):
    out, err, code = RunCommand("seqware_1.1.0-alpha.5 workflow list")
    if code is not 0:
        print err
        sys.exit()
    if search_string in out:
        print "Workflow is installed already."
        return True
    else:
        print "Workflow is not installed."
        return False

def installWorkflow():
    #os.chdir(WORKFLOW_INSTALL_DIR)
    out, err, code = RunCommand("bash installworkflow.sh")
    if code is not 0:
        print err
        sys.exit()
    return GetSwid(out)

def main():
    install = isWorkflowInstalled(WORKFLOW_SEARCH)
    if not install:
        installswid = installWorkflow()
        print "New workflow installed with SWID of %s" % (installswid)
    else:
        installswid = raw_input("Enter the workflow install SWID: ")
    UUID = raw_input("Enter the UUID of the donor to process: ")
    # ASSEMBLE COMMAND
    CMD = "%s %s %s%s/%s %s " % ( SCHEDULE_WORKFLOW_COMMAND, installswid, INI_FILE_PATH, UUID, INI_FILE_NAME, HOST_CLAUSE)
    print "Executing %s" % ( CMD )
    out, err, code = RunCommand(CMD)
    if code is not 0:
        print err
        sys.exit()
    with open(".output","w") as f:
        f.write(out)
    print "%s" % (out)
    runswid = GetSwid(out)
    
    #Turn on crontab
    with open("newcron","w") as f:
        f.write(CRON_ON)
        f.write("\n")
    out, err, code = RunCommand("crontab newcron")
    print "Crontab Turned On."
    print "watch -n 5 'seqware_1.1.0-alpha.5 workflow-run report --accession %s; qstat -f'" % (runswid)

if __name__ == '__main__':
    main()
        
        
