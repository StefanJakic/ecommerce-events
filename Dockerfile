# ============================================================
#  Jedan Dockerfile za sva tri servisa.
#  Build context je ceo projekat (root), modul se bira preko ARG.
#  docker-compose prosledjuje MODULE; rucno:
#     docker build --build-arg MODULE=order-api -t order-api .
# ============================================================

# ---- build faza ----
FROM maven:3.9.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /build
COPY pom.xml pom.xml
COPY common common
COPY order-api order-api
COPY outbox-relay outbox-relay
COPY inventory-service inventory-service
RUN mvn -q -pl ${MODULE} -am clean package -DskipTests

# ---- runtime faza ----
FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY --from=build /build/${MODULE}/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
