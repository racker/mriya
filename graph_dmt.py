#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
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

DEFAULT_GRAPH_FORMAT = 'svg'
DEFAULT_CSV_FOLDER_TO_ATTACH_TO_SVG = 'data'

def print_graph(config_file, job_files, variables, save_graph_file,
                graph_format, csvdirpath):
    list_of_job_syntax = []
    for job_file in job_files:
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
        list_of_job_syntax.append(job_syntax)

        from pprint import PrettyPrinter
        tmp_string = PrettyPrinter(indent=4).pformat(job_syntax.items())
        getLogger(LOG).info('\n'+tmp_string)
        
    graph_data = create_graph_data(list_of_job_syntax, csvdirpath, [])
    if not graph_format:
        graph_format = DEFAULT_GRAPH_FORMAT
    graph = create_displayable_graph(graph_data, graph_format)
    graph.render(save_graph_file.name)

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
    parser.add_argument("--job-file", nargs='+', action='append',
                        help="Job file with sql instructions",
                        type=file)
    parser.add_argument('--var', nargs='*', action='append')
    parser.add_argument('--logdir', action='store', required=False,
                        help='Override logdir setting')
    parser.add_argument('--save-graph', action='store', required=True,
                        type=argparse.FileType('w'),
                        help='Save transformation graph and exit')
    parser.add_argument('--format', action='store', required=False,
                        help='Save graph in format. dot/png/svg/...; svg is by default')
    parser.add_argument('--csvdir', action='store', required=False,
                        help='Only for svg format. Relative path to existing directory '
                        'with csv files, to be linked with svg image')
    return parser


if __name__ == '__main__':
    # workaround for UnicodeDecodeError
    import sys  
    reload(sys)  
    sys.setdefaultencoding('utf8')
    parser = argparse.ArgumentParser()
    parser = add_args(parser)
    args = parser.parse_args()

    variables = vars_from_args(args.var)
    if variables:
        print "Recognize variables", variables
    
    input_files = []
    for input_param in args.job_file:
        for input_file in input_param:
            input_files.append(input_file)
    if not input_files:
        print "Arguments error: Job file/s not specified"
        parser.print_help()
        exit(1)
        
    # Get logfilenae
    config = ConfigParser()
    config.read_file(args.conf_file)
    if args.logdir:
        logdirname = args.logdir
    else:
        logdirname = config[DEFAULT_SETTINGS_SECTION][LOGDIR_SETTING]
    try:
        os.makedirs(logdirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    

    loginit(STDOUT)
    loginit(STDERR)
    loginit(LOG, os.path.join(logdirname, LOG))
    getLogger(STDOUT).info("Started logging into %s", LOG)
    getLogger(STDOUT).info('Prepare graph for %s' %
                           [x.name for x in input_files])

    print_graph(args.conf_file, input_files, variables, args.save_graph,
                args.format, args.csvdir)
