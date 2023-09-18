#https://towardsdatascience.com/how-to-run-jupyter-notebook-on-docker-7c9748ed209f
FROM jupyter/base-notebook

LABEL project="Italy housing"

ENV JUPYTER_ENABLE_LAB=yes

#USER root

WORKDIR /app

COPY . /app

RUN pip install pipenv

