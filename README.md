# BareCat

BareCat (**bare**bones con**cat**enation) is a simple archive file format for storing many files,
with focus on fast random access and minimal overhead.

## Motivation

A typical use case for BareCat is storing image files for training deep learning models, where the
files are accessed randomly during training. The files are typically stored on a network file
system, where accessing many small files can be slow, and clusters often put a limit on the number
of files of a user. To avoid these problems, the files should be stored in a single archive file.
However, typical archive formats such as tar are not suitable, since they don't allow fast random
lookups. We need an index into the archive, and the index itself cannot be required to be loaded
into memory, to support very large datasets.

Therefore, in this format the metadata is indexed separately in an SQLite database for fast lookup based on paths. The index
also allows fast listing of directory contents and contains aggregate statistics (total file size,
number of files) for each directory.

## Features

- **Fast random access**: the archive can be accessed randomly, addressed by filepath,
  without having to read the entire archive into memory. The index is stored in a separate SQLite
  database, which itself does not need to be loaded entirely into memory.
- **Sharding**: to make it easier to move the data around or to distribute it across multiple
  storage devices, the archive can be split into multiple files of equal size (shards). The shards
  do not have to be concatenated to be used, the library will load data from the appropriate shard
  directly.
- **Browsability**: The SQLite database contains an index for the parent directories, allowing
  fast listing of directory contents and aggregate statistics (total file size, number of files).
- **Simple storage**: The files are simply concatenated after each other and the index contains
  the offsets and sizes of each file. The index can be dumped into json, to make it easy to
  process in any language (though SQLite is also well-supported in many languages).

## Command line interface

To create a BareCat archive, use the `barecat-create` command:

```bash
barecat-create --file mydata.barecat --shard-size 10G < path_of_paths.txt 
find . -name '*.jpg' -print0 | barecat-create --null --file mydata.barecat --shard-size 10G
```

This may yield the following files:

```
mydata.barecat-00000-of-00002
mydata.barecat-00001-of-00002
mydata.barecat-sqlite-index
```

The files can be extracted out again but metadata is lost:

```bash
barecat-extract --file mydata.barecat --target-directory targetdir/
```

## Python API

```python

import barecat

writer = barecat.Writer('mydata.barecat')
writer.add_by_content('path/to/file/as/stored.jpg', binary_file_data)
writer.add_by_path('path/to/file/on/disk.jpg')
with open('path', 'rb') as f:
    writer.add_by_fileobj('path/to/file/on/disk.jpg', f)
    
writer.close()  # or use a context manager in a `with` block

reader = barecat.Reader('mydata.barecat')
binary_file_data = reader['path/to/file.jpg']
subdirnames, filenames = reader.listdir('path/to/directory')

reader.close()  # or use a context manager in a `with` block

```

## Image Viewer

BareCat comes with a simple image viewer that can be used to browse the contents of a BareCat
archive.

```bash
barecat-image-viewer mydata.barecat
```

## Similar projects

This project is inspired by Ali Athar's file
packer https://github.com/Ali2500/TarViS/tree/main/tarvis/data/file_packer,
but there are many similar projects out there, though none seemed to match my requirements.

See for example:
- https://github.com/digidem/indexed-tarball
- https://github.com/colon3ltocard/pyindexedtar
- https://github.com/mxmlnkn/ratarmount/tree/master
- https://github.com/coelias/tarindex
- https://github.com/devsnd/tarindexer

Other alternatives include TensorFlow's TFRecord format or HDF5. However, these are more complex to use
and have many features that are not needed for this use case.


![BareCat](barecat.jpg)


 
