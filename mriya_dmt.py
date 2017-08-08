"""
Copyright (C) 2016-2017 by Yaroslav Litvinov <yaroslav.litvinov@gmail.com>
and associates (see AUTHORS).

This file is part of Mriya.

Mriya is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Mriya is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Mriya.  If not, see <http://www.gnu.org/licenses/>.
"""

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"

import argparse
import glob
import os, errno
from logging import getLogger
from configparser import ConfigParser
from sets import Set
from mriya import sql_executor
from mriya.job_syntax import *
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_controller import JobController
from mriya.log import loginit, STDOUT, STDERR, LOG, MOREINFO
from mriya.config import *

def run_job_from_file(config_file, job_file, endpoints, variables,
                      debug_steps, read_only):
    jobs_dir = os.path.dirname(job_file.name)
    macro_files = {}
    for macro_filename in glob.glob('%s/macro_*.sql' % jobs_dir):
        with open(macro_filename) as macro_file:
            macro_name = os.path.basename(macro_filename).split('.')[0]
            getLogger(LOG).info('import macros %s', macro_name)
            macro_files[macro_name] = macro_file.readlines()
    # main script data
    job_syntax = JobSyntaxExtended(job_file.readlines(),
                                   macro_files)
    restricted_ops = [x[OP_KEY] for x in job_syntax if OP_KEY in x and \
                      (x[OP_KEY] == OP_DELETE or \
                       x[OP_KEY] == OP_UPDATE or \
                       x[OP_KEY] == OP_INSERT or \
                       x[OP_KEY] == OP_UPSERT or \
                       x[OP_KEY] == OP_MERGE)]
    if read_only and restricted_ops:
        fmt_mes = "Option -read-only is specified, so \
'%s' operations can't be used in current session"
        print fmt_mes % (','.join(Set(restricted_ops)))
        exit(1)

    from pprint import PrettyPrinter
    tmp_string = PrettyPrinter(indent=4).pformat(job_syntax.items())
    getLogger(LOG).info('\n'+tmp_string)

    job_controller = JobController(
        config_file.name,
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

def jobs_from_args(args_job):
    jobs = []
    if args_job:
        for item in args_job:
            for i in item:
                jobs.append(i)
    return jobs

def add_args(parser):
    parser.add_argument("--conf-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("--job-file", nargs='*', action="append",
                        help="Job file with sql instructions",
                        type=file)
    parser.add_argument('--step-by-step', action='store_true', required=False)
    parser.add_argument('--moreinfo', action='store_true', required=False,
                        help="Display sql statement, etc. ")
    parser.add_argument('--var', nargs='*', action='append')
    parser.add_argument("--src-name",
                        help="Name of section from config related to source",
                        type=str, required=True)
    parser.add_argument("--dst-name",
                        help="Name of section from config related to dest",
                        type=str, required=True)
    parser.add_argument('--logdir', action='store', required=False,
                        help='Override logdir setting')
    parser.add_argument('--datadir', action='store', required=False,
                        help='Override datadir setting')
    parser.add_argument('-read-only', action='store_true', required=False,
                        help='Only select queries are allowed')
    
    return parser


if __name__ == '__main__':
    # workaround for UnicodeDecodeError
    import sys  
    reload(sys)  
    sys.setdefaultencoding('utf8')
    #
    parser = argparse.ArgumentParser()
    parser = add_args(parser)
    args = parser.parse_args()
    if not args.job_file:
        print "Arguments error: Job file not specified"
        parser.print_help()
        exit(1)

    jobs = jobs_from_args(args.job_file)
    variables = vars_from_args(args.var)
    endpoints = {'src': args.src_name,
                 'dst': args.dst_name}

    config = ConfigParser()
    config.read_file(args.conf_file)

    if variables:
        print "Recognize variables", variables
    
    # Get logfilenae
    if args.logdir:
        logdirname = args.logdir
    else:
        logdirname = config[DEFAULT_SETTINGS_SECTION][LOGDIR_SETTING]
    if args.datadir:
        datadirname = args.datadir
    else:
        datadirname = config[DEFAULT_SETTINGS_SECTION][DATADIR_SETTING]
    # update data path
    sql_executor.setdatadir(datadirname)
    try:
        os.makedirs(datadirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
   
    loginit(STDOUT)
    loginit(STDERR)

    for input_file in jobs:
        getLogger(STDOUT).info('Starting %s' % input_file.name)        
        # prepare log path
        logpath = os.path.join(logdirname, 
                               os.path.basename(input_file.name).split('.')[0])
        try:
            os.makedirs(logdirname)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        loginit(LOG, logpath + '.log')

        if args.moreinfo:
            loginit(MOREINFO)
        else:
            loginit(MOREINFO, LOG)
        
        run_job_from_file(args.conf_file, input_file, endpoints, variables,
                          args.step_by_step, args.read_only)

        
