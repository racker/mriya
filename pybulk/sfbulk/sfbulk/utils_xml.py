"""
Copyright (C) 2012-2013 by Clearcode <http://clearcode.cc>
and associates (see AUTHORS).

This file is part of sfbulk.

sfbulk is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

sfbulk is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with sfbulk.  If not, see <http://www.gnu.org/licenses/>.
"""
import xml.dom.minidom


# CONSTANTS FOR XML ELEMENT IDENTIFIER
ELEMENT_NODE = 1
TEXT_NODE = 3


def createxmlNode(element, value):
    """
    Utility to create XML Node Element.

    @type: string
    @param element: XML Node tag
    @type: string
    @param value: XML node tag value
    """
    xmls = xml.dom.minidom.Element(element)
    xmls.appendChild(xml.dom.minidom.Document().createTextNode(value))
    return xmls


def parseXMLResult(raw_xml):
    """
    Helper methods to transform XML to dict.

    @type: string
    @param raw_xml: XML which is represented in string
    """
    # parse the job result
    retval = {}

    parse_resp = xml.dom.minidom.parseString(raw_xml)
    Root = parse_resp.documentElement

    for child in Root.childNodes:
        if child.nodeType == ELEMENT_NODE:
            retval = _parseElement(child.childNodes, retval)
    return retval


def _parseElement(nodeElement, dataval):
    """
    Helper methods to parse each XML Element.

    @type: XMLElement
    @param nodeElement: nodeElement
    @type: dict
    @param dataval: dataval
    """
    if type(nodeElement) == xml.dom.minidom.NodeList:
        for child in nodeElement:
            _parseElement(child, dataval)
        return dataval

    if nodeElement.nodeType == TEXT_NODE:
        dataval[nodeElement.parentNode.nodeName] = nodeElement.nodeValue
        return dataval
    else:
        if nodeElement.nodeType == ELEMENT_NODE:
            _parseElement(nodeElement.childNodes, dataval)
