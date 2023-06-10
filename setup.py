from setuptools import setup

setup(
    name='barecat',
    version='0.1.0',
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
            'barecat-index-to-json=barecat.command_line_interface:index_to_json',
            'barecat-image-viewer=barecat.image_viewer:main',
        ],
    }
)
