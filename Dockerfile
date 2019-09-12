# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.
# Alpine Linux with OpenJDK JRE
FROM maven:3.5.2-jdk-8-alpine AS MAVEN_TOOL_CHAIN
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package
 
# run application with this command line 
CMD ["/usr/bin/java", "-jar", "target/k-octopus-compute.jar"]
