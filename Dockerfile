#https://towardsdatascience.com/how-to-run-jupyter-notebook-on-docker-7c9748ed209f
FROM jupyter/base-notebook

LABEL project="Italy housing"

ENV JUPYTER_ENABLE_LAB=yes

#USER root

WORKDIR /app

COPY . /app

RUN pip install pipenv

#RUN pipenv sync 

#RUN pip install --upgrade pip
#RUN pip install -r requirements.txt

# Switch back to jovyan to avoid accidental container runs as root
#USER $NB_UID

#VOLUME app/data

#EXPOSE 9000  

#CMD [ "jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root" ]


