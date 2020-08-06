Building
--------

```bash
gradle clean build
```

This will produce an tar/zip in build/distribution folder. 
The archive contains a default `lenses.conf` to pass as first argument to the application.


Running
-------

```bash
bin/generator see below

``` 


Options
-------
```json

--data 5 --topic iot_device_temperature_avro --format AVRO --brokers PLAINTEXT://broker --schema http://machine:18081

--data 5 --topic iot_device_temperature_xml --format XML --brokers PLAINTEXT://brokers  --schema http://machine:18081

--data 5 --topic iot_device_temperature_json --format JSON --brokers PLAINTEXT://broker --schema http://machine:18081

```

Available formats: JSON, XML, AVRO, PROTO
Available --data options:

 1 -> CreditCardGenerator,
 2 -> PaymentsGenerator,
 3 -> SensorDataGenerator,
 4 -> WeatherDataGenerator,
 5 -> DeviceTemperatureDataGenerator,
 6 -> DeviceTemperatureArrayDataGenerator
 
 