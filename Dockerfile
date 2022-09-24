FROM eclipse-temurin:17.0.1_12-jdk-alpine
WORKDIR /deployments
COPY target/haya-*-SNAPSHOT.jar haya.jar
RUN addgroup appuser && adduser --disabled-password appuser --ingroup appuser
RUN mkdir -p /deployments/data && chown -R appuser:appuser /deployments/data
USER appuser
CMD java $JAVA_OPTIONS -jar haya.jar
