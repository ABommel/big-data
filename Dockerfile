FROM jupyter/pyspark-notebook

MAINTAINER Pierre Navaro <pierre.navaro@univ-rennes1.fr>

USER root

COPY . ${HOME}

RUN chown -R ${NB_USER} ${HOME}

RUN conda env create --quiet -f environment.yml && \
    conda clean -tipy && \
    fix-permissions $CONDA_DIR

USER $NB_USER
