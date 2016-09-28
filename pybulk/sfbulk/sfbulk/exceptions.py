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


class BulkException(Exception):

    default_detail = u'Bulk Exception'

    def __init__(self, detail=u''):
        """
        Bulk Exception class.
        """
        super(BulkException, self).__init__()
        if detail:
            self.detail = detail

    def get_detail(self):
        return self.detail or self.default_detail

    def __str__(self):
        return self.get_detail()
