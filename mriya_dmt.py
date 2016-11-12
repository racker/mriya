#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import argparse
import glob
import os.path
from sys import stdin
from logging import getLogger
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_controller import JobController
from mriya.log import loginit, STDOUT, STDERR, LOG

def run_job_from_file(config_file, job_file, endpoints, variables,
                      debug_steps):
    jobs_dir = os.path.dirname(job_file.name)
    macro_files = {}
    for macro_filename in glob.glob('%s/macro_*.sql' % jobs_dir):
        with open(macro_filename) as macro_file:
            macro_name = os.path.basename(macro_filename).split('.')[0]
            print "import macros", macro_name
            macro_files[macro_name] = macro_file.readlines()
    # main script data
    job_syntax = JobSyntaxExtended(job_file.readlines(),
                                   macro_files)
    job_controller = JobController(config_file.name,
                                   endpoints,
                                   job_syntax,
                                   variables,
                                   debug_steps)
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
    parser.add_argument('--step-by-step', action='store_true', required=False)
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
    loginit(STDOUT)
    loginit(STDERR)
    loginit(LOG)
    getLogger(STDOUT).info('Starting')
    parser = argparse.ArgumentParser()
    parser = add_args(parser)
    args = parser.parse_args()
    if not args.job_file and not args.job_stdin:
        print "Arguments error: Job data not specified"
        parser.print_help()

    variables = vars_from_args(args.var)
    endpoints = {'src': args.src_name,
                 'dst': args.dst_name}
    input_file = None
    if args.job_stdin:
        getLogger(STDOUT).info('Run batch as stdin input')
        input_file = sys.stdin
    else:
        input_file = args.job_file
    print variables
    run_job_from_file(args.conf_file, input_file, endpoints, variables,
                      args.step_by_step)
