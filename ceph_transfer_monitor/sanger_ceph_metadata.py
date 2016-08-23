#!/usr/bin/env python

import os
import re
import sys
import csv
import json
import shutil
import datetime
from subprocess import call, check_output
from pprint import pprint as Dumper
from glob import glob

def day(delta):
    """Get the formatted date of today or yesterday"""
    if delta:
        then = datetime.datetime.today() - datetime.timedelta(delta)
        return then.strftime('%Y-%m-%d')
    else:
        return datetime.datetime.today().strftime('%Y-%m-%d')

# Shh, don't tell anyone -- for reconstructing history, to retrofit JSON changes
num1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 0
num2 = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else 1

today = day(int(num1))
yesterday = day(int(num2))
print "Today is "+today

def call_and_check(command):
    """wraps system call with retval check"""
    assert len(command) > 0, "command must not be empty!"
    print "Making system call for '"+command+"'"
    retval = call(command, shell=True)
    assert retval == 0, "Failure executing command."

def save_json(count_file, data):
    """Saves count data to a json file"""
    print "Saving JSON file "+count_file
    cwd = os.getcwd()
    os.chdir(today_path)
    json_string = json.dumps(data)
    with open(count_file, 'w') as outfile:
        outfile.write(json_string + "\n")
    file_test(count_file)
    os.chdir(cwd)
    return count_file

def file_test(count_file):
    """Sanity check for empty or non-exsistent results"""
    assert os.path.isfile(count_file), 'Error: '+count_file+' does not exists.'
    assert os.stat(count_file).st_size > 0,'Error: '+count_file+' is empty.'

def add_timepoint(timing,thing,repo,day,value):
    """Adds time point to timing data by operation/repo"""
    assert timing.keys() > 0, "This is not the timing hash I expected"
    if day not in timing[thing].keys():
        timing[thing][day] = {}
    if repo not in timing[thing][day].keys():
        timing[thing][day][repo] = []
    timing[thing][day][repo].append(value)

def add_type_count(counts,pclass,pinstance):
    """Counts files counts by project"""
    if pclass not in counts.keys():
        counts[pclass] = {}
    if pinstance not in counts[pclass].keys():
        counts[pclass][pinstance] = 0
    counts[pclass][pinstance] += 1

def add_item(projects,project):
    """Keeps track of master project list"""
    if project not in projects.keys():
        projects[project] = 1


git_base       = '/mnt/data/ceph_transfer_ops'

# Update data from github
print "Updating ceph transfer data..."
cwd = os.getcwd()
os.chdir(git_base)
if int(num1) == 0:
    call_and_check("git checkout master")
    call_and_check("git pull")
else:
    cmd = "git checkout `git rev-list -n 1 --before='"+today+" 23:59:59' master`"
    call_and_check(cmd)


types = ['ceph-transfer-jobs']
git_path       = git_base

for workflow_type in types:
    file_type = 'Sanger-VCF'

    reports        = git_path + '/ceph-metadata2/'+file_type
    today_path     = reports  + '/' + today
    yesterday_path = reports  + '/' + yesterday

    if os.path.exists(today_path):
        try:
            shutil.rmtree(today_path)
        except:
            sys.stderr.write("I seem to have a problem removing "+today_path)
        try:
            os.unlink(reports+'/latest')
        except:
            sys.stderr.write("I seem to have a problem getting rid of symlink "+reports+"/latest")
        
    os.makedirs(today_path)

    print "Mapping GNOS repos and IDs..."
    os.chdir(git_path)
    gnos_id_to_repo = {}
    all_repos = {}
    json_files = []

    json_files = glob(workflow_type+'*/*jobs/*VCF*.json')

    for jfile in json_files:

        with open(jfile) as json_file:
            try:
                json_data = json.load(json_file)
            except:
                sys.stderr.write("problem parsing "+git_path+"/"+jfile+"!\n")
                continue
        gnos_id   = json_data.get('gnos_id')
        gnos_url  = json_data.get('gnos_repo')
        if gnos_id:
            gnos_url  = str(gnos_url[0])
            gnos_repo = gnos_url.replace('https://gtrepo-','')
            gnos_repo = gnos_repo.replace('.annailabs.com/','')
            gnos_id_to_repo[gnos_id] = gnos_repo
            add_item(all_repos,gnos_repo)

    os.chdir(git_path)

    # Get the count for each queueing category
    print "Getting file counts..."
    total_file_count = {}
    file_count_project = {}
    file_count_repo = {}
    all_projects = {}
    jobs = []
    regex = re.compile('')

    jobs = glob(workflow_type+'*/*jobs')

    global_total = 0;
    for status_type in jobs:
        print "STATUS TYPE "+status_type
        all_files = glob(status_type+'/*VCF*.json')
        status_type = status_type.split('/')[1]

        files = [];
        for jfile in all_files:
            jfile = jfile.replace(workflow_type+'/','')
            files.append(jfile)

        count = len(files)
        global_total += count;
        if status_type not in total_file_count.keys():
            total_file_count[status_type] = 0
        total_file_count[status_type] += count
        #print status_type + " File count=" + str(count) + " running total="+str(global_total);
        for file_path in files:
            #print "FILEPATH "+file_path
            file = regex.sub('',file_path);
            #print "FILE HERE "+file
            pcode = file.split('.')[1]
            gnos_id = file.split('.')[0]
            gnos_id = gnos_id.split('/')[-1]
            print "GNOS ID is "+gnos_id
            gnos_repo = gnos_id_to_repo.get(gnos_id)
            print "GNOS REPO is "+gnos_repo
            add_item(all_projects,pcode)
            add_type_count(file_count_project,status_type,pcode)
            add_type_count(file_count_repo,status_type,gnos_repo)

    save_json('project_counts.json',file_count_project)
    save_json('repo_counts.json',file_count_repo)

    # Get/save project and repo lists
    hist_json = []
    yest_json = yesterday_path + '/projects.json'
    if os.path.isfile(yest_json):
        with open(yest_json) as json_file:
            hist_json = json.load(json_file)

    if len(hist_json) > 0:
        for proj in hist_json:
            add_item(all_projects,proj)
        
    projects = all_projects.keys()
    projects.sort()
    save_json('projects.json',projects)

    hist_json = []
    yest_json = yesterday_path + '/repos.json'
    if os.path.isfile(yest_json):
        with open(yest_json) as json_file:
            hist_json = json.load(json_file)

    if len(hist_json) > 0:
        for proj in hist_json:
            add_item(all_repos,proj)

    repos = all_repos.keys()
    repos.sort()
    save_json('repos.json',repos)


    # Get/save count history data
    print "Getting count history..."
    hist_json = {}
    yest_json = yesterday_path + '/hist_counts.json'
    if os.path.isfile(yest_json):
        with open(yest_json) as json_file:
            hist_json = json.load(json_file)

    hist_json[today] = total_file_count
    #Dumper(hist_json)
    save_json('hist_counts.json',hist_json)

    right_now = [datetime.datetime.today().strftime('%H:%M UTC %A, %B %d')]
    #Dumper(right_now)
    save_json('timestamp.json',right_now)

    os.chdir(reports)
    print "I am in directory"+os.getcwd()
    call_and_check("ln -sf "+today+" latest")