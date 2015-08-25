#!/usr/bin/env python

import os
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

num1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 0
num2 = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else 1

today     = day(int(num1))
yesterday = day(int(num2))
print "Today is "+today
git_base       = '/mnt/data/s3-transfer-operations/'
git_path       = git_base + 's3-transfer-jobs'
timing_path    = git_base + 'timing-information'
reports        = git_base + 's3-metadata'
today_path     = reports  + '/' + today
yesterday_path = reports  + '/' + yesterday

def call_and_check(command):
    """wraps system call with retval check"""
    assert len(command) > 0, "command must not be empty!"
    print "Making system call for '"+command+"'"
    retval = call(command, shell=True)
    assert retval == 0, "Failure executing command."

def save_json(count_file, data):
    """Saves count data to a json file"""
    print "Saving JSON file "+count_file
    csd = os.getcwd()
    os.chdir(today_path)
    json_string = json.dumps(data);
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
        timing[thing][day][repo] = [];
    timing[thing][day][repo].append(value)

def add_project_count(counts,pclass,proj):
    """Counts files counts by project"""
    if pclass not in counts.keys():
        counts[pclass] = {}
    if proj not in counts[pclass].keys():
        counts[pclass][proj] = 0
    counts[pclass][proj] += 1

def add_item(projects,project):
    """Keeps track of master project list"""
    if project not in projects.keys():
        projects[project] = 1



# Once a day
if os.path.exists(today_path):
    shutil.rmtree(today_path, ignore_errors=True)
try:
    os.unlink(reports+'/latest')
except:
    sys.stderr.write("I seem to have a problem getting rid of symlink "+reports+"/latest")
#call_and_check("ls -al "+reports)
os.makedirs(today_path)

# Update data from github
print "Updating s3 transfer data..."
cwd = os.getcwd()
os.chdir(git_path)
if int(num1) == 0:
    call_and_check("git checkout master")
    call_and_check("git pull")
else:
    call_and_check("git checkout 'master@{"+today+" 23:59:59}'")

print "Mapping GNOS repos and IDs..."
gnos_id_to_repo = {}
all_repos = {}
json_files = glob('*/*.json')
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

# Get the count for each queueing category
print "Getting file counts..."
total_file_count = {}
file_count = {}
all_projects = {}
jobs = glob('*jobs')
for status in jobs:
    files = glob(status+'/*.json')
    count = len(files)
    total_file_count[status] = count
    for file in files:
        pcode = file.split('.')[1]
        add_item(all_projects,pcode)
        add_project_count(file_count,status,pcode)
        
save_json('counts.json',file_count)

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

# Get timing information
print "Getting timing data...";
timing = {'download':{},'upload':{}}
os.chdir(timing_path);
files = glob('*.json.timing')
for timing_file in files:
    with open(timing_file) as infile:
        csv_rows = csv.reader(infile)
        for row in csv_rows:
            if row[0] == 'gnos-id':
                continue

            gnos_id = row[0]
            gnos_repo = gnos_id_to_repo.get(gnos_id)
            if not gnos_repo:
                break

            nums = [float(t) for t in list(row)[1:12]]
            (download_start,
            download_stop,
            download_delta,
            upload_start,
            upload_stop,
            upload_delta,
            workflow_start,
            workflow_stop,
            workflow_delta,
            download_size,
            upload_size) = nums

            download_day = datetime.datetime.fromtimestamp(download_start).strftime('%Y-%m-%d')
            download_rate = (download_size/download_delta)/1024
            add_timepoint(timing,'download',gnos_repo,download_day,download_rate)
            upload_day = datetime.datetime.fromtimestamp(upload_start).strftime('%Y-%m-%d')
            upload_rate = (upload_size/upload_delta)/1024
            add_timepoint(timing,'upload',gnos_repo,upload_day,upload_rate)

save_json('timing.json',timing)

# save our work
os.chdir(reports)
call_and_check("ln -sf "+today+" latest")


