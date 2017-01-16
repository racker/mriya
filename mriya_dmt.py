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
from collections import namedtuple
from mriya import sql_executor
from mriya.job_syntax import OP_KEY, OP_DELETE, OP_UPDATE, OP_UPSERT, OP_INSERT
from mriya.job_syntax import CSV_KEY, CSVLIST_KEY
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_controller import JobController
from mriya.log import loginit, STDOUT, STDERR, LOG
from mriya.config import *

def run_job_from_file(config_file, job_file, endpoints, variables,
                      debug_steps, read_only, save_graph):
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

    # graph create begin
    if save_graph:
        from pprint import PrettyPrinter
        PrettyPrinter(indent=4).pprint(job_syntax.items())
        nodes = {}
        i = 0
        GraphNodeData = namedtuple('GraphNodeData', ['id', 'edges'])
        for x in job_syntax:
            edges = []
            if CSVLIST_KEY in x:
                edges.extend(x[CSVLIST_KEY])
            if CSV_KEY in x:
                node_name = x[CSV_KEY]
                nodes[node_name] = GraphNodeData(id=i, edges=edges)
            i = i + 1
        from graphviz import Digraph
        G = Digraph(format='png')
        for k,v in nodes.iteritems():
            #G.add_node(v.id)
            G.node(str(v.id), k)
        for k,v in nodes.iteritems():
            for edge in v.edges:
                if edge in nodes:
                    G.edge(str(nodes[edge].id), str(v.id))
        G.view('/tmp/simple')
        exit(0)
        # graph end

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
    getLogger(STDOUT).info('Starting')

    run_job_from_file(args.conf_file, input_file, endpoints, variables,
                      args.step_by_step, args.read_only,
                      args.save_graph_and_exit)
