package mailbox.consumer

import org.apache.kafka.clients.consumer.*

import java.text.SimpleDateFormat

String pattern = "mm-dd-yyyy"
SimpleDateFormat dateFormat = new SimpleDateFormat(pattern)

String currentDate = dateFormat.format(new Date())

String uniqueGroupID = "automation_sales_events_consumer" + currentDate

String bootstrapServers = ""

String topicName = ""

String schemaRegistryUrl = ""
String trustStoreLocation = ""

log.info ("Adding properties to Kafka Consumer")

Properties props  = new Properties()
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupID)
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
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
props.put("auto.offset.reset", "latest")

log.info("Initializing and subscribe to the topic")

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
consumer.subscribe(Arrays.asList(topicName))
consumer.close()

log.info("Kafka consumer SetUp... with success")