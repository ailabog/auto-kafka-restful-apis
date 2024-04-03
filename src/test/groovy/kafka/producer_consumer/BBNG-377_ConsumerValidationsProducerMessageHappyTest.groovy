package kafka.producer_consumer


import java.text.SimpleDateFormat

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata

import org.apache.kafka.clients.consumer.*
import org.assertj.core.api.SoftAssertions

//Producer variables
String bootstrapServers = ""

String topicProducer = "."

String trustStoreLocation = ""
String schemaRegistryUrl = ""

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
String dateCreated = inputFormat.format(now)

String eventType= ""
String eventSubtype = ""
String eventCorrelationId = generateUUID.toString()
String eventSourceDescription = ""
String  eventSource = ""
String  documentType = ""
String  businessArea = ""
String  batchNoPfx = ""
String  objectStore = ""
String  mimeType = "application/pdf"
String  systemAddedID = ""
String  transactionID = ""
String  primaryHoldingID = ""
String  adminSystem = ""
String docClass = "Annuities_Records"
String GUID = '{' + generateUUID.toString() + '}'

//Kafka Consumer variablesto setup the props: groupId, brokers, topic, schema
String uniqueGroupID = "automation_doc_events_consumer" + currentDateComplete
String topicConsumer = ""

log.info("Adding properties to Kafka Producer")

Properties propsProducer  = new Properties()

propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
propsProducer.put("security.protocol", "")
propsProducer.put("sasl.mechanism", "")
propsProducer.put("basic.auth.credentials.source", "")
propsProducer.put("basic.auth.user.info", "")
propsProducer.put("sasl.jaas.config", "")
propsProducer.put("ssl.truststore.location", trustStoreLocation)
propsProducer.put("ssl.truststore.password", "changeit")
propsProducer.put("schema.registry.url", schemaRegistryUrl)
propsProducer.put("schema.registry.ssl.truststore.location", trustStoreLocation)
propsProducer.put("schema.registry.ssl.truststore.password", "changeit")
propsProducer.put("auto.register.schemas", false)
propsProducer.put("specific.avro.reader", true)

log.info("Producing valid Ebill message on topic: " + topicProducer)

String avroSchema = '{"fields":[{"name":"eventheader","type":{"fields":[{"doc":"enterpriseservices","name":"eventType","type":{"avro.java.string":"String","type":"string"}},{"doc":"documentaddupdate","name":"eventSubtype","type":{"avro.java.string":"String","type":"string"}},{"name":"eventDateTime","type":{"avro.java.string":"String","type":"string"}},{"name":"eventGeneratedDateTime","type":{"avro.java.string":"String","type":"string"}},{"name":"eventCorrelationId","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"eventRequestId","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"eventSourceDescription","type":[{"avro.java.string":"String","type":"string"},"null"]},{"doc":"The producer of this message.","name":"eventSource","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"metadata","type":{"fields":[],"name":"metadata","type":"record"}}],"name":"EventHeader","type":"record"}},{"name":"eventBody","type":{"fields":[{"name":"Document_Type","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"BusinessArea","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"Activity_Type","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"Batch_No_Pfx","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"SuppressImageIndicator","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"ObjectStore","type":{"avro.java.string":"String","type":"string"}},{"name":"Doc_Control_Number","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"PackageInProcess","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"MimeType","type":{"avro.java.string":"String","type":"string"}},{"name":"SystemAddedID","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"Transaction_ID","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}},{"name":"Primary_Holding_ID1","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}},{"name":"AdminSystem","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}},{"name":"TrackingNumber","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"DateCreated","type":{"avro.java.string":"String","type":"string"}},{"name":"DocClass","type":{"avro.java.string":"String","type":"string"}},{"name":"GUID","type":{"avro.java.string":"String","type":"string"}},{"name":"MajorVersion","type":{"avro.java.string":"String","type":"string"}},{"name":"MinorVersion","type":{"avro.java.string":"String","type":"string"}},{"name":"DeliveryIndicator","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"SensitivePartyIndicator","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"propertiesList","type":{"items":{"fields":[{"name":"name","type":{"avro.java.string":"String","type":"string"}},{"name":"value","type":{"avro.java.string":"String","type":"string"}},{"name":"type","type":{"avro.java.string":"String","type":"string"}},{"name":"multiValue","type":{"avro.java.string":"String","type":"string"}},{"name":"multiList","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}}],"name":"PropertyVariable","type":"record"},"type":"array"}}],"name":"EventBody","type":"record"}}],"name":"ecmDocumentPublish","namespace":"com.mm.enterprise.streaming.process","type":"record"}'

Schema.Parser parser = new Schema.Parser()
Schema avroSchemaEbill = parser.parse(avroSchema)

GenericRecord avroRecord = new GenericData.Record(avroSchemaEbill)
//  log.info(avroRecord.toString())

GenericRecord eventHeader = new GenericData.Record(avroSchemaEbill.getField("eventheader").schema())
eventHeader.put("eventType", eventType)
eventHeader.put("eventSubtype", eventSubtype)
eventHeader.put("eventDateTime", currentDateComplete)
eventHeader.put("eventGeneratedDateTime", currentDateComplete)
eventHeader.put("eventCorrelationId", eventCorrelationId)
eventHeader.put("eventRequestId", eventCorrelationId)
eventHeader.put("eventSourceDescription", eventSourceDescription)
eventHeader.put("eventSource", eventSource)
eventHeader.put("metadata", new HashMap<>())

GenericRecord eventBody = new GenericData.Record(avroSchemaEbill.getField("eventBody").schema())

eventBody.put("Document_Type", documentType)
eventBody.put("BusinessArea", businessArea)
eventBody.put("Batch_No_Pfx", batchNoPfx)
eventBody.put("ObjectStore", objectStore)
eventBody.put("MimeType", mimeType)
eventBody.put("SystemAddedID", systemAddedID)
eventBody.put("Transaction_ID", new GenericData.Array<>(avroSchemaEbill.getField("eventBody").schema().getField("Transaction_ID").schema(), Arrays.asList("ANN2023001378")))
eventBody.put("Primary_Holding_ID1", new GenericData.Array<>(avroSchemaEbill.getField("eventBody").schema().getField("Primary_Holding_ID1").schema(), Arrays.asList("000000000000000000000036603793")))
eventBody.put("AdminSystem", new GenericData.Array<>(avroSchemaEbill.getField("eventBody").schema().getField("AdminSystem").schema(), Arrays.asList("ANNWMA")))
eventBody.put("DateCreated", dateCreated)
eventBody.put("DocClass", docClass)
eventBody.put("GUID", GUID)
eventBody.put("MajorVersion", "1")
eventBody.put("MinorVersion", "0")

//propertiesList implementation
List<GenericRecord> propertiesList = new ArrayList<>()

Schema propertiesListSchema = avroSchemaEbill.getField("eventBody").schema().getField("propertiesList").schema().getElementType()
GenericRecord propertyRecord = new GenericData.Record(propertiesListSchema)

propertyRecord.put("name", "PropertyName")
propertyRecord.put("value", "PropertyValue")
propertyRecord.put("type", "PropertyType")
propertyRecord.put("multiValue", "PropertyMultiValue")
propertyRecord.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("Value1", "Value2")))
propertiesList.add(propertyRecord)
eventBody.put("propertiesList", propertiesList)

avroRecord.put("eventheader", eventHeader)
avroRecord.put("eventBody", eventBody)

log.info("message produced by Kafka Producer: " + eventHeader.toString() + eventBody.toString())

ProducerRecord<Object, Object> recordProducer = new ProducerRecord<>(topicProducer, null, avroRecord)

KafkaProducer producer = new KafkaProducer(propsProducer)

RecordMetadata metadata = producer.send(recordProducer).get()
log.info("Producer record with eventCorrelationId: " + eventCorrelationId.toString())

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

// Kafka Consumer Setup
log.info ("Adding properties to Kafka Consumer")

Properties propsConsumer  = new Properties()
propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupID)
propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
propsConsumer.put("security.protocol", "")
propsConsumer.put("sasl.mechanism", "")
propsConsumer.put("basic.auth.credentials.source", "")
propsConsumer.put("basic.auth.user.info", "")
propsConsumer.put("sasl.jaas.config", "")
propsConsumer.put("ssl.truststore.location", trustStoreLocation)
propsConsumer.put("ssl.truststore.password", "changeit")
propsConsumer.put("schema.registry.url", schemaRegistryUrl)
propsConsumer.put("schema.registry.ssl.truststore.location", trustStoreLocation)
propsConsumer.put("schema.registry.ssl.truststore.password", "changeit")
propsConsumer.put("auto.offset.reset", "latest")

log.info("Initializing and subscribing to the topic")

SoftAssertions softly = new SoftAssertions()

KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(propsConsumer)
consumer.subscribe(Arrays.asList(topicConsumer))

//Wait for 5 minutes to consume the message
try {
    Thread.sleep(5 * 60 * 1000)
} catch (InterruptedException e ) {
    Thread.currentThread().interrupt()
}

ConsumerRecords<String, GenericRecord> records = consumer.poll(1000)

for(ConsumerRecords<String, GenericRecord> record : records) {
    GenericRecord avroRecordConsum = record.value()
    log.info("Current  record value: " + record.value().toString())
    String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString()

    if(eventCorrelationId.equals(eventCorrelationIdConsumer)) {
        softly.assertThat(avroRecordConsum.get("eventHeader.eventType").toString()).IsEqualTo(eventType)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventSubtype").toString()).isEqualTo(eventSubtype)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateComplete)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateComplete)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(eventCorrelationId)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(eventCorrelationId)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventSourceDescription").to String()).isEqualTo(eventSourceDescription)
        softly.assertThat(avroRecordConsum.get("eventBody.Document_Type").toString()).isEqualTo(documentType)
        softly.assertThat(avroRecordConsum.get("eventBody.BusinessArea").toString()).isEqualTo(businessArea)
        softly.assertThat(avroRecordConsum.get("eventBody.Batch_No_Pfx").toString()).isEqualTo(batchNoPfx)
        softly.assertThat(avroRecordConsum.get("eventBody.ObjectStore").toString()).isEqualTo(objStore)
        softly.assertThat(avroRecordConsum.get("eventBody.MimeType").toString()).isEqualTo(mimeType)
        softly.assertThat(avroRecordConsum.get("eventBody.SystemAddedID").toString()).isEqualTo(systemAddedID)
        softly.assertThat(avroRecordConsum.get("eventBody.Transaction_ID").toString()).isEqualTo(transactionID)
        softly.assertThat(avroRecordConsum.get("eventBody.Primary_Holding_ID1").toString()).isEqualTo(primaryHoldingID)
        softly.assertThat(avroRecordConsum.get("eventBody.AdminSystem").toString()).isEqualTo(adminSystem)
    }
    long offset = record.offset()
    log.info("Consumed record at offset: " + offset)
}
softly.assertAll()
consumer.close()

log.info("Validating the Consumer  fields against the  fields produced... with success")