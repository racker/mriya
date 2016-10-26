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


def loadFromCSVFile(filename, max_count=10000, omit_header=False):
    """
    Simple utility to load from csv file.
    Assumption : first line will be the field name
    """
    sane_files = []
    retval = u''
    linecount = 0
    headerLines = u''
    for line in open(filename):
        retval += line
        linecount += 1
        if headerLines == u'' and not omit_header:
            headerLines = line
            linecount = 0
        if linecount >= max_count:
            linecount = 0
            sane_files.append(retval)
            retval = u''
            retval += headerLines

    if retval != u'' and retval != headerLines:
        sane_files.append(retval)
    return sane_files
