FROM jupyter/pyspark-notebook

MAINTAINER Pierre Navaro <pierre.navaro@univ-rennes2.fr>

USER root

COPY . ${HOME}

RUN chown -R ${NB_USER} ${HOME}

USER $NB_USER

RUN conda env create --quiet -f environment.yml && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER && \

ENTRYPOINT ["conda", "run", "-n", "big-data", "python", "-m",  "ipykernel",  "install", "--name",  "big-data"]
