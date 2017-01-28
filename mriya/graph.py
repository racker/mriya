#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from collections import namedtuple
from graphviz import Digraph
from mriya.job_syntax_extended import BATCH_KEY
from mriya.job_syntax import *

GraphNodeData = namedtuple('GraphNodeData', ['id', 'edges', 'shape', 'color'])
SHAPE_BOX = 'box'
SHAPE_ELLIPSE = 'ellipse'
COLOR_GREEN = 'green'
COLOR_RED = 'red'

def add_item_to_graph(item_x, idx, graph_nodes):
    edges = []
    if CSVLIST_KEY in item_x:
        edges.extend(item_x[CSVLIST_KEY])
    elif OBJNAME_KEY in item_x:
        node_name = item_x[FROM_KEY] + '.' + item_x[OBJNAME_KEY]
        edges.append(node_name)
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=[],
                                               shape=SHAPE_BOX, color=COLOR_GREEN)
        idx = idx + 1
    if CSV_KEY in item_x:
        node_name = item_x[CSV_KEY]
        print "node_name", node_name
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=edges,
                                               shape=SHAPE_ELLIPSE, color='')
        print '%s : "%s"\n' % (item_x[CSV_KEY], item_x[LINE_KEY])
        if OP_KEY in item_x:
            idx = idx + 1
            if DST_KEY in item_x:
                node2_name = 'dst:%s:%s' % (item_x[OP_KEY],
                                            item_x[DST_KEY])
            elif SRC_KEY in item_x:
                node2_name = 'src:%s:%s' % (item_x[OP_KEY],
                                            item_x[SRC_KEY])
            while node2_name in graph_nodes:
                node2_name += '.'
            graph_nodes[node2_name] \
                = GraphNodeData(id=idx, edges=[node_name],
                                shape=SHAPE_BOX, color=COLOR_RED)
            idx = idx + 1
            node3_name = item_x[NEW_IDS_TABLE]
            graph_nodes[node3_name] \
                = GraphNodeData(id=idx, edges=[node2_name],
                                shape=SHAPE_ELLIPSE, color='')
            
    idx = idx + 1
    return (idx, graph_nodes)

def create_graph_data(list_of_job_syntax):
    """ merge list of job_syntaxes into a single graph data"""
    nodes = {}
    node_id = 0
    for job_syntax in list_of_job_syntax:
        for item_x in job_syntax:
            if BATCH_KEY in item_x:
                node_name = item_x[BATCH_BEGIN_KEY][1]
                print "node_name", type(node_name), node_name            
                edges = item_x[CSVLIST_KEY]
                nodes[node_name] = GraphNodeData(id=node_id, edges=edges,
                                                 shape=SHAPE_ELLIPSE, color='')
                node_id = node_id + 1
                print edges, item_x[LINE_KEY]
                for item_nested in item_x[BATCH_KEY]:
                    node_id, nodes = add_item_to_graph(item_nested, node_id, nodes)
            else:
                node_id, nodes = add_item_to_graph(item_x, node_id, nodes)
    return nodes

def create_displayable_graph(graph_data, graph_format):
    G = Digraph(format=graph_format)
    for k,v in graph_data.iteritems():
        G.node(str(v.id), label=k,
               _attributes={'shape': v.shape,
                            'color': v.color})
    for k,v in graph_data.iteritems():
        for edge in v.edges:
            if edge in graph_data:
                G.edge(str(graph_data[edge].id), str(v.id))
    return G

