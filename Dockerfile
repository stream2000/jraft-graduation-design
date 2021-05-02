FROM openjdk:8-jdk-slim

COPY jraft-example/target/jraft-bin /jraft

WORKDIR /jraft/

ENV INIT_SERVER_LIST="localhost:8080"
ENV CONFIG_PATH=""
ENV ROLE=""

RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo 'Asia/Shanghai' >/etc/timezone

CMD sh  bin/rheakv_start.sh ${INIT_SERVER_LIST} ${CONFIG_PATH} ${ROLE}
