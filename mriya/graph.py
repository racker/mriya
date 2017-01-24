#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from collections import namedtuple
from graphviz import Digraph
from mriya.job_syntax_extended import BATCH_KEY
from mriya.job_syntax import *

GraphNodeData = namedtuple('GraphNodeData', ['id', 'edges'])

def add_item_to_graph(item_x, idx, graph_nodes):
    edges = []
    if CSVLIST_KEY in item_x:
        edges.extend(item_x[CSVLIST_KEY])
    elif OBJNAME_KEY in item_x:
        node_name = item_x[FROM_KEY] + '.' + item_x[OBJNAME_KEY]
        edges.append(node_name)
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=[])
        idx = idx + 1
    if CSV_KEY in item_x:
        node_name = item_x[CSV_KEY]
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=edges)
        print '%s : "%s"\n' % (item_x[CSV_KEY], item_x[LINE_KEY])
        if OP_KEY in item_x:
            idx = idx + 1
            if DST_KEY in item_x:
                node2_name = 'dst:%s:%s' % (item_x[OP_KEY],
                                            item_x[DST_KEY])
            elif SRC_KEY in item_x:
                node2_name = 'src:%s:%s' % (item_x[OP_KEY],
                                            item_x[SRC_KEY])
            graph_nodes[node2_name] \
                = GraphNodeData(id=idx, edges=[node_name])
    idx = idx + 1
    return (idx, graph_nodes)

def create_graph_data(job_syntax):
    nodes = {}
    node_id = 0
    GraphNodeData = namedtuple('GraphNodeData', ['id', 'edges'])
    for item_x in job_syntax:
        if BATCH_KEY in item_x:
            node_name = item_x[BATCH_BEGIN_KEY][1]
            print "node_name", type(node_name), node_name            
            edges = item_x[CSVLIST_KEY]
            nodes[node_name] = GraphNodeData(id=node_id, edges=edges)
            node_id = node_id + 1
            print edges, item_x[LINE_KEY]
            for item_nested in item_x[BATCH_KEY]:
                node_id, nodes = add_item_to_graph(item_nested, node_id, nodes)
        else:
            node_id, nodes = add_item_to_graph(item_x, node_id, nodes)
    return nodes

@staticmethod
def parse_recursive(nodes, node_id, self_values):
    res = []
    batch_items = []
    begin_counter = 0
    end_counter = 0
    for job_syntax_item in self_values:
        if BATCH_BEGIN_KEY in job_syntax_item:
            begin_counter += 1
        if BATCH_END_KEY in job_syntax_item:
            end_counter += 1
            if begin_counter == end_counter:
                batch_items.append(job_syntax_item)
        
        if begin_counter > end_counter:
            batch_items.append(job_syntax_item)
        elif begin_counter == end_counter and begin_counter != 0:
            # add all saved items, 
            # skip first batch_begin, last batch_end
            nested = JobSyntaxExtended.parse_recursive(
                batch_items[1:-1])
            batch = batch_items[0]
            batch[BATCH_KEY] = nested
            res.append(batch)
            del batch_items[:]
            begin_counter = 0
            end_counter = 0
        elif begin_counter != 0 or end_counter != 0:
            assert(0)
        else:
            res.append(job_syntax_item)
    return res


def create_displayable_graph(graph_data):
    G = Digraph(format='dot')
    for k,v in graph_data.iteritems():
        G.node(str(v.id), label=k, _attributes={'T':'some attribute'})
    for k,v in graph_data.iteritems():
        for edge in v.edges:
            if edge in graph_data:
                G.edge(str(graph_data[edge].id), str(v.id))
    return G

