## Real-Time state IoT

RIOT (Real-Time state IoT) is an application that supervises the IoT devices configured in the network. For each device state changed is possible to configure alerts for each of them, in the future the rules will be more complex as well a better alert manager. 

RIOT consumes two types of streams: states and triggers.

**States** are generated by the devices installed and configured in the network in order to know the current state of each one.

**Triggers** are rules that will match the type of the device to verify if the device state should create an alert or not.

![Architecture](docs/architecture-general.png)

## Project Structure

The project was developed using scala 2.12 and maven.

## Generate application binary

To generate the application binary you can execute: 
```
mvn clean package
```
in in the folder `/target` will exists the jars generated and could be deployed in the Flink Cluster.

## Start the Flink Cluster
To initialize the cluster execute the command:
```
docker-compose up -d
```

and will be created a Zookeeper, JobManager, TaskManager and a Kafka. The Kafka is initialized with 3 topics without replication (docker/kafka/topics.yaml).