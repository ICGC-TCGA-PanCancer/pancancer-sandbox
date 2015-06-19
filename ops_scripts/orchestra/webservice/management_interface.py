#!/usr/bin/python

__author__ = 'nbyrne'

# Simply server to expose basic seqware information to help manage endpoints

import BaseHTTPServer
import logging
import os
import shlex
import subprocess
import time
import urlparse

# CONSTANTS
DEBUG = False
PORT_NUMBER = 9009
HOST_NAME = '0.0.0.0'
LOGFILE = 'webservice.log'


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


class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """ Handler for the listener. """

    def do_HEAD(req):
        """Attach headers to a response."""
        req.send_response(200)
        req.send_header("Content-type", "text/html")
        req.end_headers()

    def do_GET(req):
        """Respond to a GET request."""
        path = urlparse.urlparse(req.path).path
        route(path, req)


def route(path, req):
    """ HTTP request router.
    Args:
        path:   The request path being made.
        req:    The request object from the server module.
    Returns:
        Nothing, handles all communication
    """
    if path == "/healthy":
        req.send_response(200)
        req.send_header("Content-type", "text/plain")
        req.end_headers()
        req.wfile.write("TRUE\n")
        return

    if path == "/workflows":
        req.send_response(200)
        req.send_header("Content-type", "text/plain")
        req.end_headers()
        if os.path.exists("/workflows"):
            results = []
            for f in os.listdir("/workflows"):
                if f == "." or f == "..":
                    continue
                if os.path.isdir(os.path.join("/workflows",f)):
                    results.append(f)
            results = sorted(results)
            for f in results:
                req.wfile.write("%s\n" % f)
            if len(f) == 0:
                req.wfile.write("None\n")
        return

    if path == "/containers":
        req.send_response(200)
        req.send_header("Content-type", "text/plain")
        req.end_headers()
        cmd = "docker ps"
        out, err, code = RunCommand(cmd)
        data = out.split('\n')
        for line in data:
            req.wfile.write("%s\n" % line)
        if code != 0:
            logging.error("ERROR RUNNING: %s" % cmd)
            logging.error("ERROR: %s" % err)
        return

    if path == "/busy":
        req.send_response(200)
        req.send_header("Content-type", "text/plain")
        req.end_headers()
        cmd = "docker ps"
        out, err, code = RunCommand(cmd)
        data = out.split("\n")
        if len(data) < 2:
            req.wfile.write("FALSE\n")
        else:
            req.wfile.write("TRUE\n")
        return

    req.send_response(404)
    req.send_header("Content-type", "text/plain")
    req.wfile.write("NOT FOUND")
    logging.error("BAD REQUEST TO PATH: %s" % path)
    return


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


def main():
    print time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)
    logging.info("Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER)
    logging.info("Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))


if __name__ == '__main__':
    setup_logging(LOGFILE)
    server_class = BaseHTTPServer.HTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
    main()
