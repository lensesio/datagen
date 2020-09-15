FROM 'adoptopenjdk/openjdk8'
COPY target/scala-2.13/datagen.jar .
ENTRYPOINT ["java", "-jar", "datagen.jar"]