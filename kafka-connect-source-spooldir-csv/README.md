```
export CLASSPATH="$(find /etc/kafka-connect/custom-plugins/kafka-connect-spooldir-1.0.31/usr/share/kafka-connect/kafka-connect-spooldir -type f -name '*.jar' | tr '\n' ':')"
```

```
kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.SchemaGenerator -t csv -f /data/connect/finished/person2.csv -c /data/connect/config/CSVExample.properties -i id
```

Create a topic `person-chg`:

```
kafka-topics --zookeeper zookeeper:2181 --create --topic person-chg --partitions 8 --replication-factor 3
```


Consume from the topic `person-chg` using `kafkacat`:

```
kafkacat -b 192.168.1.141 -t person-chg -f '%T %k - %s\n'
```


```
name=spooldir
connector.class=com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector
processing.file.extension=.PROCESSING
csv.first.row.as.header=true
finished.path=/data/connect/finished
tasks.max=1
batch.size=1000
empty.poll.wait.ms=250
halt.on.error=true
parser.timestamp.date.formats=yyyy-MM-dd'T'HH:mm:ss'Z'
file.minimum.age.ms=0
input.file.pattern=^.*.csv$
timestamp.mode=FIELD
timestamp.field=last_login
topic=person-chg
error.path=/data/connect/error
input.path=/data/connect/input
key.schema={"name":"com.trivadis.UserKey","type":"STRUCT","isOptional":false,"fieldSchemas":{"id":{"type":"INT64","isOptional":false}}}
value.schema={"name":"com.trivadis.User","type":"STRUCT","isOptional":false,"fieldSchemas":{"id":{"type":"INT64","isOptional":false},"first_name":{"type":"STRING","isOptional":true},"last_name":{"type":"STRING","isOptional":true},"email":{"type":"STRING","isOptional":true},"gender":{"type":"STRING","isOptional":true},"ip_address":{"type":"STRING","isOptional":true},"last_login":{"name":"org.apache.kafka.connect.data.Timestamp","type":"INT64","version":1,"isOptional":false},"account_balance":{"name":"org.apache.kafka.connect.data.Decimal","type":"BYTES","version":1,"parameters":{"scale":"2"},"isOptional":true},"country":{"type":"STRING","isOptional":true},"favorite_color":{"type":"STRING","isOptional":true}}}

```

create a file `configure-spoolsource.sh` and add the following script:

```
#!/bin/bash

echo "removing Spool-Dir Source Connector"

curl -X "DELETE" "$DOCKER_HOST_IP:8083/connectors/file-spooldir"

echo "creating MQTT Source Connector"

curl -X "POST" "$DOCKER_HOST_IP:8083/connectors" \
     -H "Content-Type: application/json" \
     --data '{
  "name": "file-spooldir",
  "config": {  
	  "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
	  "tasks.max": "1",
	  "topic": "person-chg",
	  "processing.file.extension": ".PROCESSING",
	  "csv.first.row.as.header": "true",
	  "input.path": "/data/connect/input",
	  "input.file.pattern": "^.*.csv$",
	  "finished.path": "/data/connect/finished",
	  "error.path": "/data/connect/error",
	  "batch.size": "1000",
	  "empty.poll.wait.ms": "250",
	  "halt.on.error": "true",
	  "parser.timestamp.date.formats": "yyyy-MM-dd'T'HH:mm:ss'Z'",
	  "timestamp.mode": "FIELD",
	  "timestamp.field": "last_login",
	  "file.minimum.age.ms": "0",
	  "key.schema": "{\"name\":\"com.trivadis.UserKey\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"id\":{\"type\":\"INT64\",\"isOptional\":false}}}",
	  "value.schema": "{\"name\":\"com.trivadis.User\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"id\":{\"type\":\"INT64\",\"isOptional\":false},\"first_name\":{\"type\":\"STRING\",\"isOptional\":true},\"last_name\":{\"type\":\"STRING\",\"isOptional\":true},\"email\":{\"type\":\"STRING\",\"isOptional\":true},\"gender\":{\"type\":\"STRING\",\"isOptional\":true},\"ip_address\":{\"type\":\"STRING\",\"isOptional\":true},\"last_login\":{\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"type\":\"INT64\",\"version\":1,\"isOptional\":false},\"account_balance\":{\"name\":\"org.apache.kafka.connect.data.Decimal\",\"type\":\"BYTES\",\"version\":1,\"parameters\":{\"scale\":\"2\"},\"isOptional\":true},\"country\":{\"type\":\"STRING\",\"isOptional\":true},\"favorite_color\":{\"type\":\"STRING\",\"isOptional\":true}}}"
	}
}'
```

```
transforms=valueToKey
transforms.valueToKey.type=org.apache.kafka.connect.transforms.ValueToKey
transforms.valueToKey.fields=country
```

```
transforms=valueToTimestamp
transforms.valueToTimestamp.type=com.trivadis.ValueToTimestamp
transforms.valueToTimestamp.field=last_login
transforms.valueToTimestamp.format=yyyy-MM-dd'T'HH:mm:ss.SSZ
```

