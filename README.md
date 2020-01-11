# Jupyter Notebook PyFilesystem Contents Manager
[![Build Status](https://travis-ci.com/manics/jupyter-pyfilesystem.svg?branch=master)](https://travis-ci.com/manics/jupyter-pyfilesystem)
[![PyPI](https://img.shields.io/pypi/v/jupyter-pyfilesystem)](https://pypi.org/project/jupyter-pyfilesystem/)

A [Jupyter Notebooks `ContentsManager`](https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#writing-a-custom-contentsmanager) that uses [PyFilesystem](https://www.pyfilesystem.org/) for storing files.
Includes a compatible [`Checkpoints`](https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#customizing-checkpoints) class.


## Installation

```
pip install jupyter-pyfilesystem
```


## Example

`jupyter_notebook_config.py`:
```python
c.NotebookApp.contents_manager_class = 'jupyter_pyfilesystem.FsContentsManager'

# In-memory temporary filesystem
c.FsContentsManager.fs_url = 'mem://'
```

See https://docs.pyfilesystem.org/en/latest/openers.html for information on how to define `fs_url`, and https://docs.pyfilesystem.org/en/latest/builtin.html for a list of built-in filesystems.
There are also several externally-contributed filesystems that can be used.
Some are listed on https://www.pyfilesystem.org/page/index-of-filesystems/

Note some filesystems may not behave as you expect.
For example, the curent implementations of the `zip://` and `tar://` filesystems do not allow you to update an existing file.
You can only create/overwrite an existing file, or open a file read-only.

For example:
```python
c.FsContentsManager.fs_url = 'zip:///tmp/test.zip'

import os
if os.path.exists(fs_url[6:]):
    c.FsContentsManager.create = False
    c.FsContentsManager.writeable = False
```

If you are using a remote filesystem you may want to enable the `keepalive`.
For example, this will make a remote request to get the details of `/` every 60 seconds:
```python
c.FsContentsManager.keepalive = 60
```

## Acknowledgements

This repository is based on https://github.com/quantopian/pgcontents/tree/5fad3f6840d82e6acde97f8e3abe835765fa824b
