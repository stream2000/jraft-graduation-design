FROM openjdk:8-jdk-slim

COPY jraft-example/target/jraft-bin /jraft

WORKDIR /jraft/

ENV INIT_SERVER_LIST="localhost:8080"
ENV CONFIG_PATH=""
ENV ROLE=""

CMD sh  bin/rheakv_start.sh ${INIT_SERVER_LIST} ${CONFIG_PATH} ${ROLE}
