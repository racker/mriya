#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import glob
import StringIO
from os.path import basename, dirname
from mriya.log import defaultlog
from mriya.opexecutor import Executor
from mriya.opmultiprocessing import FastQueueProcessor
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_controller import JobController
from mriya.graph import get_csv_files_for_entire_graph
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
            if count > 0:
                count = count -1
            aggregated_csvs[csvname] = count
    return aggregated_csvs


def run_mriya_dmt():
    executor = Executor()
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file tests/test.sql --datadir data"
    executor.execute('test_dmt', cmd, input_data=None, output_pipe=True)
    executor.poll_for_complete(None)

def get_stats_for_graph(config, sqlscript, graph_fname, csvdirpath):
    list_of_job_syntax = []
    query_template = "SELECT count() FROM csv.%s => var:COUNT"
    graphdir = dirname(graph_fname)
    with open(sqlscript) as job_file:
        jobs_dir = 'tests'
        macro_files = {}
        for macro_filename in glob.glob('%s/macro_*.sql' % jobs_dir):
            with open(macro_filename) as macro_file:
                macro_name = basename(macro_filename).split('.')[0]
                macro_files[macro_name] = macro_file.readlines()
        # main script data
        job_syntax = JobSyntaxExtended(job_file.readlines(),
                                       macro_files)
        list_of_job_syntax.append(job_syntax)
    # phase1: create graph just to get a list of csv nodes
    graph_data = create_graph_data(list_of_job_syntax, csvdirpath, [])
    csv_files = get_csv_files_for_entire_graph(graph_data,
                                               graphdir, csvdirpath)
    return aggregate_all_csvs(config, query_template, csv_files)

def update_graph_stats():
    #create new graph with statistics
    graph_data = create_graph_data(list_of_job_syntax, csvdirpath, graph_stats)
    if not graph_format:
        graph_format = DEFAULT_GRAPH_FORMAT
    graph = create_displayable_graph(graph_data, graph_format)
    graph.render(save_graph_file.name)


    
def test_graph():
    run_mriya_dmt()
    get_stats_for_graph(config='test-config.ini',
                        sqlscript='tests/test.sql',
                        graph_fname='./tests/test_graph',
                        csvdirpath='../data')

if __name__ == '__main__':
    defaultlog()
    test_graph()
