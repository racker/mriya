#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

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
from mriya.log import loginit, STDOUT, STDERR, LOG
from mriya.graph import create_graph_data
from mriya.graph import create_displayable_graph
from mriya.config import *

def run_job_from_file(config_file, job_file, endpoints, variables,
                      debug_steps, read_only, save_graph_file):
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
                       x[OP_KEY] == OP_UPSERT)]
    if read_only and restricted_ops:
        fmt_mes = "Option -read-only is specified, so \
'%s' operations can't be used in current session"
        print fmt_mes % (','.join(Set(restricted_ops)))
        exit(1)

    from pprint import PrettyPrinter
    tmp_string = PrettyPrinter(indent=4).pformat(job_syntax.items())
    getLogger(LOG).info('\n'+tmp_string)

    if save_graph_file:
        graph_data = create_graph_data(job_syntax)
        graph = create_displayable_graph(graph_data)
        graph.view(save_graph_file.name)
        exit(0)

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


def add_args(parser):
    parser.add_argument("--conf-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("--job-file", action="store",
                        help="Job file with sql instructions",
                        type=file)
    parser.add_argument('--step-by-step', action='store_true', required=False)
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
    parser.add_argument('--save-graph-and-exit', action='store', required=False,
                        type=argparse.FileType('w'),
                        help='Save transformation graph and exit')
    
    return parser


if __name__ == '__main__':
    # workaround for UnicodeDecodeError
    import sys  
    reload(sys)  
    sys.setdefaultencoding('utf8')
    parser = argparse.ArgumentParser()
    parser = add_args(parser)
    args = parser.parse_args()
    if not args.job_file:
        print "Arguments error: Job file not specified"
        parser.print_help()
        exit(1)

    variables = vars_from_args(args.var)
    endpoints = {'src': args.src_name,
                 'dst': args.dst_name}
    input_file = args.job_file
    if variables:
        print "Recognize variables", variables

    # Get logfilenae
    config = ConfigParser()
    config.read_file(args.conf_file)
    if args.logdir:
        logdirname = args.logdir
    else:
        logdirname = config[DEFAULT_SETTINGS_SECTION][LOGDIR_SETTING]
    if args.datadir:
        datadirname = args.datadir
    else:
        datadirname = config[DEFAULT_SETTINGS_SECTION][DATADIR_SETTING]
    # update data path
    sql_executor.DATADIRNAME = datadirname
    # prepare log path
    logpath = os.path.join(logdirname, 
                           os.path.basename(input_file.name).split('.')[0])

    try:
        os.makedirs(logdirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    try:
        os.makedirs(datadirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

    loginit(STDOUT)
    loginit(STDERR)
    loginit(LOG, logpath + '.log')
    getLogger(STDOUT).info('Starting %s' % input_file.name)

    run_job_from_file(args.conf_file, input_file, endpoints, variables,
                      args.step_by_step, args.read_only,
                      args.save_graph_and_exit)
