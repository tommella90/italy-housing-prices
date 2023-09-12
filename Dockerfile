#https://towardsdatascience.com/how-to-run-jupyter-notebook-on-docker-7c9748ed209f
ARG BASE_CONTAINER=jupyter/minimal-notebook
FROM $BASE_CONTAINER

USER root

WORKDIR /app

COPY . /app

RUN pip install pipenv
#RUN pipenv sync

#RUN pip install --upgrade pip
#RUN pip install -r requirements.txt

# Switch back to jovyan to avoid accidental container runs as root
#USER $NB_UID

#VOLUME app/data

EXPOSE 8888  

CMD [ "jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root" ]
