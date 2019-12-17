# Jupyter Notebook OMERO Contents Manager

A [Jupyter Notebooks ContentsManager](https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#writing-a-custom-contentsmanager) that uses [OMERO](https://www.openmicroscopy.org/omero/) for storing files.



## Example


`jupyter_notebook_config.py`:
```
c.NotebookApp.contents_manager_class = 'jupyter_omero_contents.OmeroContentsManager'
c.ContentsManager.checkpoints_class = 'jupyter_omero_contents.OmeroCheckpoints'

c.OmeroContentsManager.omero_host = 'omero.example.org'
c.OmeroContentsManager.omero_user = 'username'
c.OmeroContentsManager.omero_password = 'password'

c.OmeroCheckpoints.omero_host = 'omero.example.org'
c.OmeroCheckpoints.omero_user = 'username'
c.OmeroCheckpoints.omero_password = 'password'
```
If you need an OMERO session:
```
omero login
omero sessions list
```
