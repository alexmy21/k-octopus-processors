# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.
# Alpine Linux with OpenJDK JRE
FROM openjdk:8-jre-alpine
# copy WAR into image
COPY target/k-octopus-compute-0.7.2-jar-with-dependencies.jar /k-octopus-compute.jar 
# run application with this command line 
CMD ["/usr/bin/java", "-jar", "/k-octopus-compute.jar"]
