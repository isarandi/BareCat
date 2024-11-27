from setuptools import setup

setup(
    name='barecat',
    version='0.1.2',
    author='István Sárándi',
    author_email='istvan.sarandi@gmail.com',
    packages=['barecat'],
    license='LICENSE',
    description='Efficient dataset storage format through barebones concatenation of binary files '
                'and an SQLite index. Optimized for fast random access in machine learning '
                'workloads.',
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'barecat-create=barecat.command_line_interface:create',
            'barecat-extract=barecat.command_line_interface:extract',
            'barecat-merge=barecat.command_line_interface:merge',
            'barecat-merge-symlink=barecat.command_line_interface:merge_symlink',
            'barecat-extract-single=barecat.command_line_interface:extract_single',
            'barecat-index-to-csv=barecat.command_line_interface:index_to_csv',
            'barecat-verify=barecat.command_line_interface:verify_integrity',
            'barecat-viewer=barecat.viewerqt6:main',
        ],
    },
    install_requires=[
        'multiprocessing-utils'
    ],
)
