FROM openjdk:8-jdk-alpine

COPY jraft-example/target/jraft-bin /jraft

WORKDIR /jraft/bin/

ENV INIT_SERVER_LIST=""
ENV CONFIG_PATH=""
ENV ROLE=""

CMD sh rheakv_start.sh ${INIT_SERVER_LIST} ${CONFIG_PATH} ${ROLE}
