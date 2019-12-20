FROM jupyter/base-notebook:cd158647fb94
# https://hub.docker.com/layers/jupyter/base-notebook/cd158647fb94/images/sha256-c9df73049562ac22bfa572e6ee7a37c55b7608d33fa0330020f6b247d28570e6
# 12/9/2019 at 1:53 pm

# Bug in notebook 6.0.0 with custom content managers
# https://github.com/jupyter/notebook/pull/4891
RUN conda install -y -q --freeze-installed -c ome omero-py 'notebook>=6.0.2'

USER root
ADD . /jupyter-omero-contents/
RUN cd /jupyter-omero-contents && python3 setup.py bdist_wheel
USER jovyan
RUN python3 -mpip install /jupyter-omero-contents/dist/*whl

RUN echo "c.NotebookApp.contents_manager_class = 'jupyter_omero_contents.OmeroContentsManager'" >> \
        ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.ContentsManager.checkpoints_class = 'jupyter_omero_contents.OmeroCheckpoints'" >> \
        ~/.jupyter/jupyter_notebook_config.py
