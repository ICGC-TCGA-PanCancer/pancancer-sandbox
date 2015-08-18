import os
import re
import shlex
import subprocess
import sys
import time
import urlparse

# Constants
POLL_INTERVAL = 10.0
PROGRESS_REGEX = r'(\d+.\d+)% complete'


class DownloadConfig(object):
    """ A class representing configuration needed to manage a successful download. """
    def __init__(self, remote_url, gnos_key, timeout):
        self.remote_url = remote_url
        self.gnos_key = gnos_key
        self.timeout = timeout
        # Extract the download folder name
        parsed_url = urlparse.urlparse(remote_url)
        self.download_folder = os.path.basename(parsed_url.path)


def argument_error():
    """ Prints an error message to stdout in the event that the utility is called without proper parameters.
    :return: None
    """
    print "USAGE: python friendly-gnos.py [REMOTE URL] [GNOS KEYFILE] [INACTIVITY TIMEOUT MINUTES]"
    print ""
    sys.exit(1)


def gtdownload(config):
    """ A wrapper around the gtdownload binary, parses the output indicator.
    :param config:  A DownloadConfig object
    :return: The exitcode of the system call.
    """
    cmd = "gtdownload -vc %s %s" % (config.gnos_key, config.remote_url)
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    idle_time = 0
    last_poll_cycle = time.time()
    last_completion = 0.0
    while p.poll() is None:
        # Don't read data too quickly, allow POLL_INTERVAL to pass
        if time.time() - last_poll_cycle < POLL_INTERVAL:
            continue
        # Read all stdout, print to stdout and store as a variable
        stdout = p.stdout.read()
        if len(stdout) > 0:
            sys.stdout.write(stdout)
        # Read all stderr, print to stderr and store as a variable
        stderr = p.stderr.read()
        if len(stderr) > 0:
            sys.stderr.write(stderr)
        # Follow Adam's method of parsing stdout for completion percentage, and note the difference
        for line in stdout.split('\n'):
            match = re.search(PROGRESS_REGEX, line)
            if match is not None:
                current_completion = float(match.group(1))
            if current_completion == last_completion or match is None:
                if idle_time == 0:
                    idle_time == time.time()
                else:
                    inactivity = time.time() - idle_time
                    if (inactivity / 60.0) > float(config.timeout):
                        print "=-" * 40
                        print "DOWNLOAD HAS BEEN INACTIVE FOR %s MINUTES, TERMINATING..." % config.timeout
                        p.kill()
            else:
                idle_time = 0
    return p.returncode


def main():
    config = DownloadConfig(sys.argv[1], sys.argv[2], sys.argv[3])
    errcode = gtdownload(config)
    sys.exit(errcode)


if __name__ == '__main__':
    # Parse Arguments
    if len(sys.argv) != 4:
        argument_error()
    main()
