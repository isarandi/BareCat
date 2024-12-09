import numpy as np
from setuptools import Extension, setup

# To compile and install locally run "python setup.py build_ext --inplace"
# To install library to Python site-packages run "python setup.py build_ext install"
ext_modules = [
    Extension(
        'barecat.fuse.wrapper',
        sources=['barecat/fuse/wrapper.pyx'],
        extra_compile_args=['-O3', '-Wno-cpp', '-Wno-unused-function', '-std=c99'],
        define_macros=[("FUSE_USE_VERSION", "39")],
        libraries=["fuse3", "c"]
    ),
    Extension(
        'barecat.cython.barecat_cython',
        sources=['barecat/cython/barecat_cython.pyx', 'barecat/cython/barecat.c',
                 'barecat/cython/barecat_mmap.c', 'barecat/cython/crc32c.c'],
        extra_compile_args=['-O3', '-Wno-cpp', '-Wno-unused-function', '-std=c11'],
        include_dirs=[np.get_include(), 'barecat/cython'],
        define_macros=[("SQLITE_THREADSAFE", "2")],
        libraries=["sqlite3", "c"]
    )
]

setup(ext_modules=ext_modules)
