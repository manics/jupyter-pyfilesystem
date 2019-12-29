# Jupyter Notebook PyFilesystem Contents Manager

A [Jupyter Notebooks ContentsManager](https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#writing-a-custom-contentsmanager) that uses [PyFilesystem](https://www.pyfilesystem.org/) for storing files.


## Example

`jupyter_notebook_config.py`:
```
c.NotebookApp.contents_manager_class = 'jupyter_pyfilesystem.FsContentsManager'
c.ContentsManager.checkpoints_class = 'jupyter_pyfilesystem.FsCheckpoints'

# In-memory temporary filesystem
fs_url = 'mem://'

c.FsContentsManager.fs_url = fs_url
c.FsCheckpoints.fs_url = fs_url
```

See https://docs.pyfilesystem.org/en/latest/openers.html for information on how to define `fs_url`, and https://docs.pyfilesystem.org/en/latest/builtin.html for a list of built-in fiesystems.
There are several externally-contributed filesystems that can be used.
Some are listed on https://www.pyfilesystem.org/page/index-of-filesystems/
