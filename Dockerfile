FROM openjdk:11

COPY  target/consumeri3s-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
#COPY ./scripts/ /bin



