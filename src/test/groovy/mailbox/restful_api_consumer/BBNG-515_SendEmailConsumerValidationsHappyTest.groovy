package mailbox.restful_api_consumer


import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.avro.generic.GenericRecord

import java.text.SimpleDateFormat

import okhttp3.MediaType
import org.assertj.core.api.SoftAssertions

String base_url_ecorr = ""
String middle_url_ecorr = ""
String end_url_ecorr = ""

String URL_ECORR = base_url_ecorr + middle_url_ecorr + end_url_ecorr
String username = ""
String password = ""

//Define Consumer variables

String pattern = "mm-dd-yyyy"
SimpleDateFormat dateFormat = new SimpleDateFormat(pattern)

String currentDate = dateFormat.format(new Date())

String uniqueGroupID = "automation_sales_events_consumer" + currentDate

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("dd MMM yyyy HH:mm:ss")
String currentDateCompleteDay = date.format(now)

String bootstrapServers = ""

String topicConsumer = ""

String schemaRegistryUrl = ""
String trustStoreLocation = ""
String eventType = ""
String eventSourceDescription = "Monitoring"
String messageContent = "Automated email sent with success"
String phoneNo = "+12672133096"

RequestBody  ecorrRequestBody = RequestBody.create(MediaType.parse("application/json"), '{"messageType":"email","sender":{"replyTo":"","alias":"","email":""},"messageFormat":"text","recipients":[{"recipientType":"to","email":""}],"applicationId":"","content":{"items":[{"templateType":"text","index":0,"text":"+12672133096","contentType":"emailSubject"},{"templateType":"text","index":1,"text":"Automated email sent with success","contentType":"emailBody"}]}}')

OkHttpClient client = new OkHttpClient()
Request request_ecorr = new Request.Builder().url(URL_ECORR).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(ecorrRequestBody).build()
log.info("Ecorr Call:" + request_ecorr)

Response response_ecorr = client.newCall(request_ecorr).execute()
String responseBody_ecorr= response_ecorr.body().string()
int statusCode_ecorr = response_ecorr.code()

log.info("Status code for Ecorr Call:" + statusCode_ecorr)
log.info("Response for Ecorr:" + responseBody_ecorr.toString())

log.info ("Addding properties to Kafka Consumer")

Properties props  = new Properties()
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupID)
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
props.put("security.protocol", "")
props.put("sasl.mechanism", "")
props.put("basic.auth.credentials.source", "")
props.put("basic.auth.user.info", "")
props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='svc3etosaleevtest' password='C8Z7trOrIBan';")
props.put("ssl.truststore.location", trustStoreLocation)
props.put("ssl.truststore.password", "changeit")
props.put("schema.registry.url", schemaRegistryUrl)
props.put("schema.registry.ssl.truststore.location", trustStoreLocation)
props.put("schema.registry.ssl.truststore.password", "changeit")
props.put("auto.offset.reset", "latest")

log.info("Initializing and subscribe to the topic")

log.info("Kafka consumer SetUp... with success")

SoftAssertions softly = new SoftAssertions()

KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)
consumer.subscribe(Arrays.asList(topicConsumer))
ConsumerRecords<String, GenericRecord> records = consumer.poll(1000)

for(ConsumerRecords<String, GenericRecord> record : records) {
    GenericRecord avroRecordConsum = record.value()
    log.info("Current  record value: " + record.value().toString())
    String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString()

    if(eventCorrelationId.equals(eventCorrelationIdConsumer)) {
        softly.assertThat(avroRecordConsum.get("eventHeader.eventType").toString()).IsEqualTo(eventType)

        softly.assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateCompleteDay)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateCompleteDay)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(eventCorrelationId)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(eventCorrelationId)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventSourceDescription").to String()).isEqualTo(eventSourceDescription)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventSource").to String()).isEqualTo(eventType)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventInitiatorDescription").to String()).isEqualTo(eventType)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventInitiator").to String()).isEqualTo(eventType)
        softly.assertThat(avroRecordConsum.get("eventBody.messageContent").toString()).isEqualTo(messageContent)
        softly.assertThat(avroRecordConsum.get("eventBody.phoneNumber").toString()).isEqualTo(phoneNumber)
    }
    long offset = record.offset()
    log.info("Consumed record at offset: " + offset)
}
softly.assertAll()
consumer.close()

log.info("Validating the Consumer  fields against the  emails template fields... with success")