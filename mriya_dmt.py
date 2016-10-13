#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import argparse
from configparser import ConfigParser
from mriya.job_syntax import JobSyntax
from mriya.job_controller import JobController
from mriya.log import loginit

def create_job_controller(config, job_file, src_name, dst_name):
    endpoint_names = {'src': src_name,
                      'dst': dst_name}
    job_syntax = JobSyntax(job_file.readlines())
    job_controller = JobController(config,
                                   endpoint_names,
                                   job_syntax)
    job_controller.run_job()

def add_args(parser):
    parser.add_argument("--conf-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("--job-file", action="store",
                        help="Job file with sql instructions",
                        type=file, required=True)
    parser.add_argument("--src-name",
                        help="Name of section from config related to source",
                        type=str, required=True)
    parser.add_argument("--dst-name",
                        help="Name of section from config related to dest",
                        type=str, required=True)
    return parser


if __name__ == '__main__':
    # workaround for UnicodeDecodeError
    import sys  
    reload(sys)  
    sys.setdefaultencoding('utf8')
    loginit(__name__)
    parser = argparse.ArgumentParser()
    parser = add_args(parser)
    args = parser.parse_args()
    config = ConfigParser()
    config.read_file(args.conf_file)
    create_job_controller(config, args.job_file,
                          args.src_name, args.dst_name)

