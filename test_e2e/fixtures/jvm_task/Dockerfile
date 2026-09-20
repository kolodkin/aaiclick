# jvm-task fixture image for the docker-runner e2e.
#
# Build context (assembled by the ``jvm_task_image`` fixture):
#   sdk/  the checkout's java/ tree — aaiclick-task-api at the commit under test
#   app/  this directory — the user's @AaiTask project
#
# The runtime stage is the shape docs/designs/java-sdk.md prescribes for a
# user image: the shim as ENTRYPOINT, so the runner's ``--task-id N
# --run-epoch M`` arguments reach it.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY sdk /build/sdk
RUN mvn -B -q -f /build/sdk/pom.xml -Drevision=0.0.0-e2e -DskipTests install

COPY app /build/app
RUN mvn -B -q -f /build/app/pom.xml package

FROM eclipse-temurin:21-jre
COPY --from=build /build/app/target/app.jar /app/app.jar
COPY --from=build /build/app/target/libs /app/libs
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/libs/*", "io.github.kolodkin.aaiclick.task.AaiTaskShim"]
