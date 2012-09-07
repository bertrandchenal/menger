#!/usr/bin/env python

#This file is part of Menger. The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license
#terms.

from distutils.core import setup

setup(name='Menger',
      version='1.1',
      description='Statistics storage',
      author='Bertrand Chenal',
      author_email='bertrandchenal@gmail.com',
      url='https://bitbucket.org/bertrandchenal/menger',
      packages=['menger'],
      requires=['leveldb'],
      license='ISC',
      classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
      )
