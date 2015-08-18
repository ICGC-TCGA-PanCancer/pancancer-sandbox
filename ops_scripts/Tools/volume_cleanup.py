import os
import sys

from boto import ec2
import boto

def credentials():
    return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
            "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}

def GetInstances(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region,
                              **creds)
        instances = []
        reservations = conn.get_all_reservations()
        for reservation in reservations:
            for instance in reservation.instances:
                instances.append(instance)
    except boto.exception.EC2ResponseError:
        return []
    return instances

def GetVolumes(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region,
                              **creds)
        volumes = conn.get_all_volumes()
    except boto.exception.EC2ResponseError:
        return []
    return volumes

def GetRegions():
    regions = ec2.regions()
    region_names = []
    for region in regions:
        region_names.append(region.name)
    return region_names

def main():
    regions = GetRegions()
    
    print "UNATTACHED VOLUMES"
    print "Name:\tStatus:\tSize:\tCreated:\tZone:\tSnapShot ID:"
    for region in regions:
        volumes = GetVolumes(region)
        for v in volumes:
            if v.attachment_state() is not None:
                continue
            print "%s\t%s\t%sGB\t%s\t%s\t%s" % (v.id, v.status, v.size, v.create_time, v.zone, v.snapshot_id)
            # print "DELETING THIS VOLUME ... "
            #result = v.delete()
            #if result is True:
            #    print "\tDELETED!"
            #else:
            #    print "\tERROR!"
            
    print "\n\nVOLUMES ATTACHED TO STOPPED INSTANCES"
    for region in regions:
        instances = GetInstances(region)
        volumes = GetVolumes(region)
        for v in volumes:
            if v.attachment_state() is None:
                continue
            for instance in instances:
                if instance.id == v.attach_data.instance_id:
                    if instance.state != 'running':
                        print "%s\t%s\t%sGB\t%s\t%s\t%s\t%s" % (v.id, v.status, v.size, v.create_time, v.zone, v.snapshot_id, instance.id)
    
    

if __name__ == '__main__':
    main()
