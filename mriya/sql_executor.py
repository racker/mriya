#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from mriya.job_syntax import JobSyntax 

class SqlExecutor(object):
    def __init__(self, job_syntax_item, variables):
        self.job_syntax_item = job_syntax_item
        self.variables = variables

    def execute(self, query):
        raise NotImplementedError("'execute' method not defined!")
    
    
