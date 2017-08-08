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

import os
from sets import Set
from collections import namedtuple
from graphviz import Digraph
from mriya.sql_executor import SqlExecutor
from mriya.job_syntax_extended import BATCH_KEY
from mriya.job_syntax import *

GraphNodeData = namedtuple('GraphNodeData',
                           ['id', 'edges', 'shape', 'color', 'style', 'info', 'href'])
SHAPE_STAR = 'star'
SHAPE_BOX = 'box'
SHAPE_ELLIPSE = 'ellipse'
COLOR_GREEN = 'green'
COLOR_RED = 'red'
STYLE_DASHED = 'dashed'
BAD_NODE_STYLE = 'diagonals'
BAD_NODE = "Can't locate entity in script / External dependency"
EXTERNAL_OBJECT_READ='Read from salesforce object'
EXTERNAL_OBJECT_WRITE='Write into salesforce object'
EXTERNAL_OBJECT_RESULT="List of ids as result of operation on Salesforce object"

def get_href_info(item_x, node_name, csvdir, aggregated_csvs):
    csvhref = ''
    nodeinfo = ''
    if CSV_KEY in item_x and csvdir and len(csvdir):
        csvhref = '%s/%s.csv' % (csvdir, node_name)
    if LINE_KEY in item_x:
        if node_name in aggregated_csvs:
            nodeinfo = 'count=%d;' % aggregated_csvs[node_name]
    return (csvhref, nodeinfo)


def add_item_to_graph(item_x, idx, graph_nodes, csvdir, aggregated_csvs):
    edges = []
    # get csv relations
    if CSVLIST_KEY in item_x:
        edges.extend(item_x[CSVLIST_KEY])
    elif OBJNAME_KEY in item_x:
        node_name = item_x[FROM_KEY] + '.' + item_x[OBJNAME_KEY]
        edges.append(node_name)
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=[],
                                               shape=SHAPE_BOX,
                                               color=COLOR_GREEN,
                                               style='',
                                               info=EXTERNAL_OBJECT_READ,
                                               href='')
        idx = idx + 1
    # get var relations
    if QUERY_KEY in item_x:
        edges.extend(SqlExecutor.get_query_var_names(item_x[QUERY_KEY]))

    csvhref, nodeinfo = ('','')

    # var nodes
    if VAR_KEY in item_x:
        node_name = item_x[VAR_KEY]
        color=COLOR_GREEN
        csvhref, nodeinfo = get_href_info(item_x, node_name, csvdir, aggregated_csvs)
        nodeinfo += 'query: %s' % item_x[LINE_KEY]
        if PUBLISH_KEY in item_x:
            color=COLOR_RED
        if node_name in graph_nodes:
            edges.extend(graph_nodes[node_name].edges)
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=list(Set(edges)),
                                               shape=SHAPE_ELLIPSE,
                                               style=STYLE_DASHED,
                                               color=color,
                                               info=nodeinfo,
                                               href=csvhref)
        print '%s : "%s"\n' % (item_x[VAR_KEY], item_x[LINE_KEY])
        idx = idx + 1
    # csv nodes  
    elif CSV_KEY in item_x:
        node_name = item_x[CSV_KEY]
        csvhref, nodeinfo = get_href_info(item_x, node_name, csvdir, aggregated_csvs)
        nodeinfo += 'query: %s' % item_x[LINE_KEY]
        if node_name in graph_nodes:
            edges.extend(graph_nodes[node_name].edges)
        graph_nodes[node_name] = GraphNodeData(id=idx, edges=list(Set(edges)),
                                               shape=SHAPE_ELLIPSE,
                                               color='',
                                               style='',
                                               info=nodeinfo,
                                               href=csvhref)
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
                                shape=SHAPE_BOX, color=COLOR_RED,
                                style='',
                                info=EXTERNAL_OBJECT_WRITE,
                                href='')
            idx = idx + 1
            # add node as result of operation
            node3_name = item_x[NEW_IDS_TABLE]
            csvhref, nodeinfo = get_href_info(item_x, node3_name, csvdir,
                                              aggregated_csvs)
            nodeinfo += EXTERNAL_OBJECT_RESULT
            graph_nodes[node3_name] \
                = GraphNodeData(id=idx, edges=[node2_name],
                                shape=SHAPE_ELLIPSE, color='',
                                style='',
                                info=nodeinfo,
                                href=csvhref)
            
    idx = idx + 1
    return (idx, graph_nodes)

def create_graph_data(list_of_job_syntax, csvdir, aggregated_csvs):
    """ merge list of job_syntaxes into a single graph data"""
    nodes = {}
    node_id = 0
    for job_syntax in list_of_job_syntax:
        for item_x in job_syntax:
            if BATCH_KEY in item_x:
                node_name = item_x[BATCH_BEGIN_KEY][1]
                edges = item_x[CSVLIST_KEY]
                nodes[node_name] = GraphNodeData(id=node_id, edges=edges,
                                                 shape=SHAPE_ELLIPSE, color='',
                                                 style='',
                                                 info='',
                                                 href='')
                node_id = node_id + 1
                print edges, item_x[LINE_KEY]
                for item_nested in item_x[BATCH_KEY]:
                    node_id, nodes = add_item_to_graph(item_nested, node_id, nodes,
                                                       csvdir, aggregated_csvs)
            else:
                node_id, nodes = add_item_to_graph(item_x, node_id, nodes,
                                                   csvdir, aggregated_csvs)
    return nodes

def add_warning_for_absent_nodes(graph_data):
    # add non existing nodes as specially marked
    id_non_existing_node = 10000
    absent_nodes = {}
    for k,v in graph_data.iteritems():
        for edge in v.edges:
            if edge not in graph_data:
                absent_nodes[edge] \
                    = GraphNodeData(id=id_non_existing_node, edges=[],
                                    shape=SHAPE_BOX, color=COLOR_RED,
                                    style=BAD_NODE_STYLE,
                                    info=BAD_NODE,
                                    href='')
                id_non_existing_node += 1
    graph_data.update(absent_nodes)
    return graph_data

def create_displayable_graph(graph_data, graph_format):
    graph_data = add_warning_for_absent_nodes(graph_data)
    G = Digraph(format=graph_format)
    # adding nodes
    for nodename,v in graph_data.iteritems():
        G.node(str(v.id), label=nodename,
               _attributes={'shape': v.shape,
                            'color': v.color,
                            'style': v.style,
                            'tooltip': v.info.replace(',', ', '),
                            'href': v.href})
    # adding edges
    for nodename,v in graph_data.iteritems():
        for edge in v.edges:
            if edge in graph_data:
                G.edge(str(graph_data[edge].id), str(v.id))
    return G

