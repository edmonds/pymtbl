#!/usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension

try:
    from Cython.Distutils import build_ext
    setup(
        name = 'mtbl',
        ext_modules = [ Extension('mtbl', ['mtbl.pyx'], libraries = ['mtbl']) ],
        cmdclass = {'build_ext': build_ext},
    )
except ImportError:
    setup(
        name = 'mtbl',
        ext_modules = [ Extension('mtbl', ['mtbl.c'], libraries = ['mtbl']) ],
    )
