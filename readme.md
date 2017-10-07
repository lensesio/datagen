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
$configurationFile  $topic $option
```

Available options:
 - 1 -  credit card data
 - 2 -  payments data



Config
------
This is the format of the configuration file the generator expects:
```json
brokers=""
schema.registry=""
format="avro"
```

`format` - can be avro or json.
