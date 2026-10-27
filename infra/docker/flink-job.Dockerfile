# Builds (and unit-tests, incl. the shared rule vectors) the JVM modules, then bakes the jar into the Flink image.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY jvm/pom.xml jvm/pom.xml
COPY jvm/rules-java/pom.xml jvm/rules-java/pom.xml
COPY jvm/flink-common/pom.xml jvm/flink-common/pom.xml
COPY jvm/kappa-flink-job/pom.xml jvm/kappa-flink-job/pom.xml
COPY jvm/lambda-flink-speed/pom.xml jvm/lambda-flink-speed/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f jvm/pom.xml dependency:go-offline -DskipTests || true
COPY jvm jvm
COPY shared/rules shared/rules
RUN --mount=type=cache,target=/root/.m2 mvn -B -f jvm/pom.xml -pl kappa-flink-job,lambda-flink-speed -am package

FROM flink:1.20.2-java17
COPY --from=build /build/jvm/kappa-flink-job/target/kappa-flink-job.jar /opt/jobs/kappa-flink-job.jar
COPY --from=build /build/jvm/lambda-flink-speed/target/lambda-flink-speed.jar /opt/jobs/lambda-flink-speed.jar
COPY shared/config/demo.yaml /config/demo.yaml
