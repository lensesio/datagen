# Lenses Datagen 

A cli tool to generate test data for Lenses and its supported data sources.

## Buildin from source

In order to build Datagen from source you will need to install the Google protobuf compiler. Please ensure the `protoc` executable is in your `PATH`.
With this installed, you should be able to build a standalone executable Jar by running:

```
sbt assembly
```

When the operation completes, the jar will be available under `target/scala-2.13/`.

## Available generators

The following generators are available as cli subcommands:

- _records_: given a topic, a dataset and a format, ingest auto-generated records in the topic
- _schemas_: generates an arbitrary number of topics and configure them with a randomly generated schema.

## Schema generator

This generator is aimed at testing Lenses Explore screen and its search functionality. It will create the supplied number of topics/indices/tables and autogenerate random schemas matching the constraints of the source data system. Notice that currently this generator creates empty datasets and ingests no record.

### Usage:

```bash
java -jar datagen.jar gen-schemas
  --num-kafka-datasets  <int>
        Number of kafka topics to generate
  --num-elastic-datasets  <int>
        Number of Elasticsearch indexes to generate
  --num-postgres-datasets  <int>
        Number of Postgres tables to generate
  --lenses-base-url  <URI>
        Lenses base URL (defaults to http://localhost:24015)
  --lenses-creds  <user:password>
        Lenses user credentials (defaults to admin:admin)
  --lenses-basic-auth-creds  <user:password?>
        Lenses basic auth creds (needed for running against traefik) (defaults to none)
  --elastic-base-url  <URI>
        Elasticsearch base URL (defaults to http://localhost:9200)
  --elastic-creds  <user:password?>
        Elasticsearch creds (defaults to None)
  --postgres-schema  <string?>
        Create Postgres tables within a schema (defaults to none)
  --postgres-database  <string?>
        Postgres database (required to generate postgres tables)
  --postgres-creds  <user:password>
        Postgres credentials (defaults to 'postgres:'')

```

## Record generator

Data generators that will ingest multiple records into the supplied Kafka topics. A set of predefined generators and schema is supported and can be generated in multiple wire formats.

### Usage

```bash
gen-records --data-set 5 --topic iot_device_temperature_avro --format AVRO --brokers PLAINTEXT://broker --schema http://machine:18081
```

Available formats: JSON, XML, AVRO, PROTO
Available --data-set options:

```
 1  -> CreditCardGenerator,
 2  -> PaymentsGenerator,
 3  -> SensorDataGenerator,
 4  -> WeatherDataGenerator,
 5  -> DeviceTemperatureDataGenerator,
 6  -> DeviceTemperatureArrayDataGenerator
 7  -> SubscriptionGenerator,
 8  -> StationsGenerator,
 9  -> TripsGenerator,
 10 -> CustomerGenerator,
 11 -> OrdersGenerator,
 12 -> Big message (almost 0.5Mb) generator,
 13 -> Large message (almost 1Mb) generator,
 14 -> Nested message generator
 ```

Generation of nested messages (14) fails for AVRO - avro schema can not be created and pushed to the Schema Registry.

## Legacy generators

This project initially contained generators for a number of data systems that are not yet supported by Lenses (e.g JMS, Pulsar, Hive). In order to simplify the build, these have been recently removed. However, they can be can be easily accessed through [this commit](https://github.com/lensesio/datagen/commit/3aefd7870a066588a44e7a5e79a734012a2a0e9d) and might be reinstated at a later stage.