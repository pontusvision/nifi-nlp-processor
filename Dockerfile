FROM maven:3.6-jdk-8-alpine as builder
COPY pom.xml /nifi-nlp-processor/
WORKDIR /nifi-nlp-processor
COPY nifi-nlp-nar/pom.xml /nifi-nlp-processor/nifi-nlp-nar/
COPY nifi-nlp-processors/pom.xml /nifi-nlp-processor/nifi-nlp-processors/
 #   git  clone --depth 1  --single-branch --branch master https://github.com/pontusvision/nifi-nlp-processor.git && \

RUN cd nifi-nlp-processors && \
    mvn -q -B dependency:resolve

COPY . /nifi-nlp-processor/

RUN mvn -q clean package -U -DskipTests

FROM scratch
#RUN   mkdir -p /opt/nifi/nifi-current/lib
COPY --from=builder /nifi-nlp-processor/nifi-nlp-nar/target/*.nar /opt/nifi/nifi-current/lib/

