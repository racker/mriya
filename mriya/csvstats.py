#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import glob
import StringIO
from os.path import basename, dirname, isfile, getsize, join
from mriya.log import defaultlog
from mriya.opexecutor import Executor
from mriya.opmultiprocessing import FastQueueProcessor
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_controller import JobController
from mriya.graph import create_graph_data
from mriya.graph import create_displayable_graph
from mriya import sql_executor

def async_worker_run_query(conf_datadir_and_template, csvname):
    """ function intended to call by FastQueueProcessor.
    process mongo record / bson data in separate process.
    It is expects just one query, so only first query will be executed """
    conf = conf_datadir_and_template[0]
    datadir = conf_datadir_and_template[1]
    query_template = conf_datadir_and_template[2]
    endpoints = {}
    variables = {}
    strbuf = StringIO.StringIO(query_template % csvname)
    job_syntax = JobSyntaxExtended(strbuf.readlines(), [])
    sql_executor.DATADIRNAME = datadir
    job_controller = JobController(
        conf,
        endpoints,
        job_syntax,
        variables,
        None)
    job_controller.run_job()
    return {csvname:job_controller.variables['COUNT']}

def aggregate_all_csvs(config, query_template, csv_files):
    aggregated_csvs = {}
    datadir = None
    if csv_files:
        datadir = dirname(csv_files[0])
    async_engine = FastQueueProcessor(async_worker_run_query,
                                      (config, datadir, query_template),
                                      4)
    for csvpath in csv_files:
        name = basename(csvpath).split('.')[0]
        async_engine.put(name)
    while not async_engine.error and async_engine.is_any_working():
        res = async_engine.get()
        if res:
            csvname = res.keys()[0]
            count = int(res[csvname])
            aggregated_csvs[csvname] = count
    return aggregated_csvs

def aggregate_csvs(sqlscripts, csvdirpath):
    """ Return dictionary like {csv_graph_node_name: count, ...} """
    list_of_job_syntax = []
    query_template = "SELECT count() FROM csv.%s => var:COUNT"
    for sqlscript in sqlscripts:
        with open(sqlscript) as job_file:
            jobs_dir = dirname(sqlscript)
            macro_files = {}
            for macro_filename in glob.glob('%s/macro_*.sql' % jobs_dir):
                with open(macro_filename) as macro_file:
                    macro_name = basename(macro_filename).split('.')[0]
                    macro_files[macro_name] = macro_file.readlines()
            # main script data
            job_syntax = JobSyntaxExtended(job_file.readlines(),
                                       macro_files)
        list_of_job_syntax.append(job_syntax)
    # create graph just to get a list of csv nodes
    graph_data = create_graph_data(list_of_job_syntax, csvdirpath, [])
    csv_files = get_csv_files_for_entire_graph(graph_data, csvdirpath)
    return aggregate_all_csvs(None, query_template, csv_files)

def get_csv_files_for_entire_graph(graph_data, csvdatapath):
    """ Get list of existing csv files """
    csv_paths = []
    for nodename in graph_data:
        candidate_path = join(csvdatapath, nodename + '.csv')
        if isfile(candidate_path) and getsize(candidate_path) > 0:
            csv_paths.append(candidate_path)
    return csv_paths
