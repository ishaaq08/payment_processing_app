FROM eclipse-temurin:25
WORKDIR /tmp
ENV BROKER_ADDRESS=broker:9092
COPY ./target/transaction-producer-1.0-SNAPSHOT.jar .
CMD ["java", "-jar", "transaction-producer-1.0-SNAPSHOT.jar"]