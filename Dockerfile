FROM docker-dev.hli.io/ccm/hli-rspark:2.0.0

ENV APP_DIR /app_root
RUN mkdir -p ${APP_DIR}

WORKDIR ${APP_DIR}

# python requirements add && install
ADD requirements.txt ${APP_DIR}
RUN pip install py4j && pip install -r requirements.txt

# env for pyspark;
ENV PYTHONPATH $SPARK_HOME/python/:$PYTHONPATH

# copy app
COPY ./app ${APP_DIR}/app

# add entrypoint
ADD ./entry_point.sh ${APP_DIR}
ENTRYPOINT ["./entry_point.sh"]
