#!/usr/bin/python

# Parses the output of oozie jobs to diagnose a job failure, and dump the error files

import glob
import re
import shlex
import subprocess
import sys

GENERATED_SCRIPTS_PATH = "/glusterfs/data/ICGC3/seqware_results_icgc/scratch/"

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

def ParseOozieJob(jobname):
    out, err, code = RunCommand("oozie job -info %s" % (jobname))
    if code != 0:
        print err
        sys.exit()
    lines = out.split('\n')
    directory = None
    for line in lines:
        match = re.search(r"\S+@(?P<STEP>\S+)\s+ERROR\s+\d+\s+\S+\s(?P<ERRORCODE>\S+)", line, re.DOTALL)
        if match:
            return directory, match.group("ERRORCODE"), match.group("STEP")
            continue
        match = re.search(r"App Path      : hdfs://master:8020/user/BOCONNOR/seqware_workflow/(\S+)", line)
        if match:
            directory = match.group(1)

def main():
    # Find the error in the job, and dump the error files
    directory, errcode, stepname = ParseOozieJob(sys.argv[1])
    print "ERROR: %s STEP: %s " % (errcode, stepname)
    raw_input("Press ENTER to dump error files...")
    f = "%s%s/generated-scripts/*%s.e*" % (GENERATED_SCRIPTS_PATH, directory, stepname)
    fl = glob.glob(f)
    for f in fl:
        with open(f) as data:
            print 10*"="
            print f
            print 10*"-"
            print data.read()
            print 10*"-"
            print ""
            



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "You need to specify an oozie job."
    main()