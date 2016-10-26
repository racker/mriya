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
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='sfbulk',
    version='1.0',
    description='Salesforce Bulk API handling with Python',
    url='https://github.com/clearcode/sfbulk',
    license='GPL',
    author='Natalia Frydrych',
    author_email='n.frydrych@clearcode.cc',
    packages=['sfbulk'],
    install_requires=[
        'requests == 2.3.0'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: SalesForce',
        'Topic :: Software Development :: Libraries'
    ]
)
