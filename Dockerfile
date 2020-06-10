FROM flink:1.10.0-scala_2.12

RUN curl https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.12.tgz -o /opt/flink-1.10.0-bin-scala_2.12.tgz
RUN tar -xzf /opt/flink-1.10.0-bin-scala_2.12.tgz
RUN mv flink-1.10.0 /opt

RUN rm -rf /opt/flink/*
RUN rm -rf /opt/flink/plugins/*
RUN rm -rf /opt/flink/lib/*

RUN cp -R /opt/flink-1.10.0/* /opt/flink

RUN mv -v /opt/flink/plugins/flink-queryable-state-runtime_2.12-1.10.0.jar /opt/flink/lib/

RUN chown -R flink:flink /opt/flink/lib/