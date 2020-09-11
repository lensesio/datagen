# Lenses Datagen 

A cli tool to generate test data for Lenses and its supported data sources.

## Available generators

The following generators are available:

- records: given a topic, a dataset and a format, ingest auto-generated records in the topic
- schemas: generates an arbitrary number of topics and configure them with a randomly generated schema.

## Schema generator

This generator is aimed at testing Lenses Explore screen and its search functionality. It will create the supplied number of topics/indices/tables and autogenerate random schemas matching the constraints of the source data system. Notice that currently this generator creates empty datasets and ingests no record.

### Usage:

```bash
Usage: datagen gen-schemas
  --num-kafka-datasets  <int>
        Number of kafka topics to generate
  --num-elastic-datasets  <int>
        Number of Elasticsearch indexes to generate
  --lenses-base-url  <URI>
        Lenses base URL (defaults to http://localhost:24015)
  --lenses-creds  <user:password>
        Lenses user credentials (defaults to admin:admin)
  --elasticsearch-baseurl  <URI>
        Elasticsearch base URL (defaults to http://localhost:9200)


```

## Record generator

Legacy data generators that will ingest multiple randomly generated records into the supplied Kafka topics. A set of predefined schemas is supported and can be generated in multiple wire formats.

### Usage

```bash
gen-records --data-set 5 --topic iot_device_temperature_avro --format AVRO --brokers PLAINTEXT://broker --schema http://machine:18081
```

Available formats: JSON, XML, AVRO, PROTO
Available --data-set options:

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

## Legacy generators

This project initially contained generators for a number of data systems that are not yet supported by Lenses (e.g JMS, Pulsar, Hive). In order to simplify the build, these have been recently removed. However, they can be can be easily accessed through [this commit](https://github.com/lensesio/datagen/commit/3aefd7870a066588a44e7a5e79a734012a2a0e9d) and might be reinstated at a later stage.