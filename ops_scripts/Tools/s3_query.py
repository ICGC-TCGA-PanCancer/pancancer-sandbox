import boto
import ConfigParser
import logging
import os
import sys
        

def ReadS3Cfg(filename=os.path.join(os.environ['HOME'], ".s3cfg")):
    """ Read credentials from s3cmd config file which doesn't conform to standard ini format :(. """
    logging.info('Reading credentials from ~/.s3cfg')
    with open(filename) as f:
        data = f.readlines()
        
    config = {}
    for line in data:
        parsed = line.split(' = ')
        if len(parsed) > 1:
            config[parsed[0].strip()] = parsed[1].strip()
    
    return {"aws_access_key_id": config['access_key'],
            "aws_secret_access_key": config['secret_key']}

def SetupLogging(filename,level=logging.INFO):
    logging.basicConfig(filename=filename,level=level)

def Credentials():
    """ Read Credentials from environment variables """
    return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
            "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}

def GetS3TopLevel(bucket):
    key_list = bucket.list("", "/")
    for k in key_list:
        print k.name
        logging.info("FOUND: %s" % k.name)

def main():
    SetupLogging('s3_query.log')
    bucket_name = sys.argv[1]
    creds = ReadS3Cfg()
    try:
        logging.info("Connecting to S3 ... ")
        conn = boto.connect_s3(creds['aws_access_key_id'], creds['aws_secret_access_key'])
        logging.info("Connecting to Bucket: %s ... " % (bucket_name))
        bucket = conn.get_bucket(bucket_name)
        logging.info("Getting list of keys ... ")
        GetS3TopLevel(bucket)
        conn.close()
    except Exception as e:
        logging.ERROR(str(e)+"\n\n")
        sys.stderr.write(str(e)+"\n\n")
        sys.stderr.write("Error interfacing with S3.\n")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage:\n s3upload.py bucketname\n"
        sys.exit(1)
    main()