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
bin/generator lenses.conf credit_card_detail 1
``` 


Options
-------
```json

Data Generator Arguments
--data 5 --topic iot_device_temperature_avro --format AVRO --brokers PLAINTEXT://cloudera01.landoop.com:19092,PLAINTEXT://cloudera02.landoop.com:19092 --schema http://cloudera02.landoop.com:18081

--data 5 --topic iot_device_temperature_xml --format XML --brokers PLAINTEXT://cloudera01.landoop.com:19092,PLAINTEXT://cloudera02.landoop.com:19092 --schema http://cloudera02.landoop.com:18081

--data 5 --topic iot_device_temperature_json --format JSON --brokers PLAINTEXT://cloudera01.landoop.com:19092,PLAINTEXT://cloudera02.landoop.com:19092 --schema http://cloudera02.landoop.com:18081

```

