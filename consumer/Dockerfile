FROM eclipse-temurin:25
WORKDIR /tmp
ENV BROKER_ADDRESS=broker:9093
COPY ./target/consumer-1.0-SNAPSHOT.jar .
CMD ["java", "-jar", "consumer-1.0-SNAPSHOT.jar"]