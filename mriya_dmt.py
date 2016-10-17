#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import argparse
import json
from sys import stdin
from logging import getLogger
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_controller import JobController
from mriya.log import loginit

def run_job_from_file(config, job_file, endpoints, variables):
    job_syntax = JobSyntaxExtended(job_file.readlines())
    job_controller = JobController(config,
                                   endpoints,
                                   job_syntax,
                                   variables)
    job_controller.run_job()

def run_job_from_stdin(config, endpoints, variables):
    getLogger(__name__).info('Run batch as stdin input')
    job_syntax = json.load(stdin)
    job_controller = JobController(config,
                                   endpoints,
                                   job_syntax,
                                   variables)
    job_controller.run_job()


def vars_from_args(args_var):
    variables = {}
    if args_var:
        for item in args_var:
            variables[item[0]] = item[1]
    return variables


def add_args(parser):
    parser.add_argument("--conf-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("--job-file", action="store",
                        help="Job file with sql instructions",
                        type=file)
    parser.add_argument('--job-stdin', action='store_true', required=False)
    parser.add_argument('--var', nargs='*', action='append')
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
    if not args.job_file and not args.job_stdin:
        print "Arguments error: Job data not specified"
        parser.print_help()

    variables = vars_from_args(args.var)
    endpoints = {'src': args.src_name,
                 'dst': args.dst_name}
    if args.job_stdin:
        run_job_from_stdin(args.conf_file, endpoints, variables)        
    else:
        run_job_from_file(args.conf_file, args.job_file,
                          endpoints, variables)
