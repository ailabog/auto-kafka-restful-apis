package kafka.db_producer


import java.text.SimpleDateFormat
import org.json.JSONObject
import org.json.JSONArray

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.ResultSet
import org.assertj.core.api.SoftAssertions

//Producer variables

String topicName = ""

String bootstrapServers = ""
String schemaRegistryUrl = ""
String trustStoreLocation = ""

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

//MySQL variables
String connectionUrl = ""
String username = ""
String password = "&mr"
String queryMessage = ''
String applicationIdExpected = ""
String messageType = ""

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
props.put("sasl.jaas.config","")
props.put("ssl.truststore.location", trustStoreLocation)
props.put("ssl.truststore.password", "changeit")
props.put("schema.registry.url", schemaRegistryUrl)
props.put("schema.registry.ssl.truststore.location", trustStoreLocation)
props.put("schema.registry.ssl.truststore.password", "changeit")
props.put("auto.register.schemas", false)
props.put("specific.avro.reader", true)

log.info("Producing valid Ebill message on topic: " + topicName)

String avroSchema = '{"fields":[{"name":"eventheader","type":{"fields":[{"doc":"","name":"eventType","type":{"avro.java.string":"String","type":"string"}},{"doc":"","name":"eventSubtype","type":{"avro.java.string":"String","type":"string"}},{"name":"eventDateTime","type":{"avro.java.string":"String","type":"string"}},{"name":"eventGeneratedDateTime","type":{"avro.java.string":"String","type":"string"}},{"name":"eventCorrelationId","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"eventRequestId","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"eventSourceDescription","type":[{"avro.java.string":"String","type":"string"},"null"]},{"doc":"The producer of this message.","name":"eventSource","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"metadata","type":{"fields":[],"name":"metadata","type":"record"}}],"name":"EventHeader","type":"record"}},{"name":"eventBody","type":{"fields":[{"name":"Document_Type","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"BusinessArea","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"Activity_Type","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"Batch_No_Pfx","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"SuppressImageIndicator","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"ObjectStore","type":{"avro.java.string":"String","type":"string"}},{"name":"Doc_Control_Number","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"PackageInProcess","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"MimeType","type":{"avro.java.string":"String","type":"string"}},{"name":"SystemAddedID","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"Transaction_ID","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}},{"name":"Primary_Holding_ID1","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}},{"name":"AdminSystem","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}},{"name":"TrackingNumber","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"DateCreated","type":{"avro.java.string":"String","type":"string"}},{"name":"DocClass","type":{"avro.java.string":"String","type":"string"}},{"name":"GUID","type":{"avro.java.string":"String","type":"string"}},{"name":"MajorVersion","type":{"avro.java.string":"String","type":"string"}},{"name":"MinorVersion","type":{"avro.java.string":"String","type":"string"}},{"name":"DeliveryIndicator","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"SensitivePartyIndicator","type":[{"avro.java.string":"String","type":"string"},"null"]},{"name":"propertiesList","type":{"items":{"fields":[{"name":"name","type":{"avro.java.string":"String","type":"string"}},{"name":"value","type":{"avro.java.string":"String","type":"string"}},{"name":"type","type":{"avro.java.string":"String","type":"string"}},{"name":"multiValue","type":{"avro.java.string":"String","type":"string"}},{"name":"multiList","type":{"items":[{"avro.java.string":"String","type":"string"},"null"],"type":"array"}}],"name":"PropertyVariable","type":"record"},"type":"array"}}],"name":"EventBody","type":"record"}}],"name":"","namespace":".","type":"record"}'

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
eventBody.put("Primary_Holding_ID1", new GenericData.Array<>(avroSchemaEbill.getField("eventBody").schema().getField("Primary_Holding_ID1").schema(), Arrays.asList("00000000000009455985")))
eventBody.put("AdminSystem", new GenericData.Array<>(avroSchemaEbill.getField("eventBody").schema().getField("AdminSystem").schema(), Arrays.asList("OPM")))
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

ProducerRecord<Object, Object> record = new ProducerRecord<>(topicName, null, avroRecord)

KafkaProducer producer = new KafkaProducer(props);

RecordMetadata metadata = producer.send(record).get()

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

log.info("Event correlation Id: " +  eventHeader.get("eventCorrelationId") + " " + "GUID: "  + eventBody.get("GUID") +  "  " +  "Primary Holding id:" + eventBody.get("Primary_Holding_ID1") + " " + "Admin system: " + eventBody.get("AdminSystem"))

//DB validations

Connection conn = DriverManager.getConnection(connectionUrl, username, password)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setString(1, eventCorrelationId)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    String type = rs.getString("type")
    String source = rs.getString("source")
    String payload = rs.getString("payload")
    String received_date = rs.getString("received_date")

    log.info("Validating the DB payload:")

    JSONObject jsonPayload = new JSONObject(payload)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    String originalEventCorrelationID = sharedData.getString("originalEventCorrelationID")
    String applicationId = jsonPayload.getString("applicationId")
    String overwriteMessageType = sharedData.getString("overwriteMessageType")
    String originalEventInitiatorDescription = sharedData.getString("originalEventInitiatorDescription")
    String originalApplicationId = sharedData.getString("originalApplicationId")
    String policyNumber = sharedData.getString("policyNumber")
    String originalEventRequestID = sharedData.getString("originalEventRequestID")
    String originalEventSourceDescription = sharedData.getString("originalEventSourceDescription")
    String originalEventSource = sharedData.getString("originalEventSource")
    String originalEventInitiator = sharedData.getString("originalEventInitiator")
    String originalEventDateTime = sharedData.getString("originalEventDateTime")

    String fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName")
    String sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound")
    String templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType")
    String sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType")

    log.info("Validating the db fields against the  fields produced... with success")

    SoftAssertions softly = new SoftAssertions()

    if (originalEventCorrelationID.equals(eventCorrelationId)) {

        softly.assertThat(applicationId).isEqualTo(applicationIdExpected)
        softly.assertThat(overwriteMessageType).isEqualTo(messageType)
        softly.assertThat(originalEventInitiatorDescription).isEqualTo(eventSourceDescription)
        softly.assertThat(policyNumber).isEqualTo(primaryHoldingID)
        softly.assertThat(originalEventSourceDescription).isEqualTo(eventSourceDescription)
        softly.assertThat(originalEventSource).isEqualTo("digital-communications-orchestrator")
        softly.assertThat(originalEventRequestID).isEqualTo(eventCorrelationId)
        softly.assertThat(originalEventDateTime).isEqualTo(currentDateComplete)
        softly.assertThat(policyNumber).isEqualTo(primaryHoldingID)
        softly.assertThat(originalEventInitiator).isEqualTo(applicationId)
        softly.assertThat(fileName).isEqualTo("ebill-push-ws-title")
        softly.assertThat(templateType).isEqualTo("thymeleaf")
        softly.assertThat(sourceType).isEqualTo("aurora")
        JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess")
        String DbmessageType = jsonPayload.getString("messageType")
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }
}
conn.close()

log.info("Validating the db fields against the  fields produced for: " + documentType + " and type is:  " + messageType +  "... with success")