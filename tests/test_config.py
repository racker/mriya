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

__author__ = 'Yaroslav Litvinov'

from configparser import ConfigParser
from io import BytesIO

options = ['consumer_key', 'consumer_secret', 'user', 'pass', 'host_prefix']
sections = {'src': options, 'dst': options}

def get_text_config_lines():
    res = []
    for section_name in sections:
        res.append('[%s]\n' % section_name)
        for optname in sections[section_name]:
            res.append('%s = %s_%s_abcxyz\n' % \
                       (optname, section_name, optname))
    return res

def get_file_object_config():
    res_fp = BytesIO()
    txt_lines = get_text_config_lines()
    print txt_lines
    res_fp.writelines(txt_lines)
    res_fp.seek(0)
    return res_fp

def test_config_parse():
    # prepare
    config = ConfigParser()
    config.read_file(get_file_object_config())
    # test
    assert len(options)
    assert len(sections)
    for section_name in sections:
        for optname in sections[section_name]:
            conf_value = config.get(section_name, optname)
            expected = '%s_%s_abcxyz' % (section_name, optname)
            assert conf_value == expected
