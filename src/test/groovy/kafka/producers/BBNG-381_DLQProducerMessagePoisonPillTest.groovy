package kafka.producers


import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

String bootstrapServers = ""

String topicName = ""

String trustStoreLocation = ""
String schemaRegistryUrl = ""

log.info("Adding properties to Kafka Producer")

Properties props  = new Properties()

props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
props.put("security.protocol", "")
props.put("sasl.mechanism", "")
props.put("basic.auth.credentials.source", "")
props.put("basic.auth.user.info", "")
props.put("sasl.jaas.config", "")
props.put("ssl.truststore.location", trustStoreLocation)
props.put("ssl.truststore.password", "changeit")
props.put("schema.registry.url", schemaRegistryUrl)
props.put("schema.registry.ssl.truststore.location", trustStoreLocation)
props.put("schema.registry.ssl.truststore.password", "changeit")
props.put("auto.register.schemas", false)
props.put("specific.avro.reader", true)

log.info("Producing valid message on topic: " + topicName)

log.info("Producing a null message")

ProducerRecord<Object, Object> record = new ProducerRecord<>(topicName, null, null)

KafkaProducer producer = new KafkaProducer(props);

producer.send(record)

producer.flush()
producer.close()

log.info("Poison pill was sent on dlq topic: " + topicName + " ... with success")