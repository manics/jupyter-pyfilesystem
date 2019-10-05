# Jupyter Notebook OMERO Contents Manager

A [Jupyter Notebooks ContentsManager](https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#writing-a-custom-contentsmanager) that uses [OMERO](https://www.openmicroscopy.org/omero/) for storing files.



## Example


`jupyter_notebook_config.py`:
```
c.NotebookApp.contents_manager_class = 'jupyter_omero_contents.OmeroContentsManager'
c.ContentsManager.checkpoints_class = 'notebook.services.contents.filecheckpoints.GenericFileCheckpoints'
```
You must export these two environment variables before running jupyter-notebook.
```
export OMERO_HOST=omero.example.org
export OMERO_SESSION=omero-session-uuid
jupyter-notebook
```
If you need an OMERO session:
```
omero login
omero sessions list
```


## Known issues
- Checkpoints aren't enabled
- Viewing files doesn't work (editting text and notebook files works)
- Renaming files works but results in an error message saying it failed
