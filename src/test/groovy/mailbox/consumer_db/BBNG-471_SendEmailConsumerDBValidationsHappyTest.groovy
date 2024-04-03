package mailbox.consumer_db


import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.avro.generic.GenericRecord

import java.text.SimpleDateFormat

import org.assertj.core.api.SoftAssertions
import okhttp3.MediaType
import org.json.JSONArray
import org.json.JSONObject

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.ResultSet

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

//Define ecorr db
String connectionUrl = ""
String usernameDB = ""
String passwordDB = ""
String querySMSCount = ''
String queryEmailCount = ''
String applicationIdExpected = ""

log.info("Creating  Call ..." + URL_ECORR)

RequestBody  ecorrRequestBody = RequestBody.create(MediaType.parse("application/json"), '{"messageType":"email","sender":{"replyTo":"","alias":"","email":""},"messageFormat":"text","recipients":[{"recipientType":"to","email":""}],"applicationId":"","content":{"items":[{"templateType":"text","index":0,"text":"+12672133096","contentType":"emailSubject"},{"templateType":"text","index":1,"text":"Automated email sent with success today","contentType":"emailBody"}]}}')

OkHttpClient client = new OkHttpClient()
Request request_ecorr = new Request.Builder().url(URL_ECORR).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(ecorrRequestBody).build()
log.info("Call:" + request_ecorr)

Response response_ecorr = client.newCall(request_ecorr).execute()
String responseBody_ecorr= response_ecorr.body().string()
int statusCode_ecorr = response_ecorr.code()

//Extract values for transExecDateTS/msgTblIdent from response message micro call
JSONObject jsonSendEmailObject = new JSONObject(responseBody_ecorr)
String transExecValue = jsonSendEmailObject.getString("transExecDateTS")
String msgTblIdent = jsonSendEmailObject.getString("msgTblIdent")
String correlationId = jsonSendEmailObject.getString("correlationId")

//Extracting fields from the request payload for comparison
JSONObject requestPayloadJson = new JSONObject('{"messageType":"email","sender":{"replyTo":"","alias":"","email":""},"messageFormat":"text","recipients":[{"recipientType":"to","email":""}],"applicationId":"","content":{"items":[{"templateType":"text","index":0,"text":"+12672133096","contentType":"emailSubject"},{"templateType":"text","index":1,"text":"Automated email sent with success today","contentType":"emailBody"}]}}')

String emailExpected = requestPayloadJson.getJSONObject("sender").getString("email")
String replyToExpected = requestPayloadJson.getJSONObject("sender").getString("replyTo")
String textSubjectExpected = requestPayloadJson.getJSONObject("content").getJSONArray("items").getJSONObject(0).getString("text")
String textBodyExpected = requestPayloadJson.getJSONObject("content").getJSONArray("items").getJSONObject(1).getString("text")

log.info("Status code for Call:" + statusCode_ecorr)
log.info("Response for:" + responseBody_ecorr.toString())

log.info("Set up the Consumer...")
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

log.info("Connecting to db and start the validations..one record for email, one record for sms")

//DB validations
log.info("Connecting to db and start the validations..one record for email, one record for sms")

Connection conn = DriverManager.getConnection(connectionUrl, usernameDB, passwordDB)

PreparedStatement stmnSMS = conn.prepareStatement(querySMSCount)
PreparedStatement stmnEmail = conn.prepareStatement(queryEmailCount)
stmnEmail.setString(1, transExecValue)
stmnSMS.setString(1, transExecValue)

ResultSet rsEmail = stmnEmail.executeQuery()
ResultSet rsSMS= stmnSMS.executeQuery()

rsEmail.next()
int emailCount = rsEmail.getInt(1)
log.info("Email count:" +emailCount)

rsSMS.next()
int smsCount = rsSMS.getInt(1)
log.info("SMS count:" +smsCount)

if(emailCount == 1 && smsCount == 1 ){
    log.info("One sms record and one email record found into the db")
} else {
    log.info("Something went wrong")
}

if(rsEmail.next() && rsSMS.next()) {

    //Extract fields from email payload
    String typeDB = rsEmail.getString("type")
    String sourceDB = rsEmail.getString("source")
    String jsonPayload = rsEmail.getString("payload")
    String received_date = rsEmail.getString("received_date")
    JSONObject payloadJsonEmail = new JSONObject(jsonPayload)

    JSONObject sender = payloadJsonEmail.getJSONObject("sender")
    String emailDB = sender.getString("email")
    String replyToDB = sender.getString("replyTo")
    String applicationId = payloadJsonEmail.getString("applicationId")

    JSONObject content = payloadJsonEmail.getJSONObject("content")
    JSONArray items = content.getJSONArray("items")
    String emailSubjectDB = items.getJSONObject(0).getString("text")
    String emailBodyDB = items.getJSONObject(1).getString("text")

    //Extract fields from sms payload
    String payloadJsonSMS = rsSMS.getString("payload")
    JSONObject payloadJsonSMSObject = new JSONObject(payloadJsonSMS)

    String applicationIdSMS = payloadJsonSMSObject.getString("applicationId")
    JSONObject contentSMS = payloadJsonSMSObject.getJSONObject("content")
    String textSMS = contentSMS.getJSONArray("items").getJSONObject(0).getString("text")

    SoftAssertions softly = new SoftAssertions()

    KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)
    consumer.subscribe(Arrays.asList(topicConsumer))
    ConsumerRecords<String, GenericRecord> records = consumer.poll(1000)

    for(ConsumerRecords<String, GenericRecord> record : records) {
        GenericRecord avroRecordConsum = record.value()
        log.info("Current  record value: " + record.value().toString())
        String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString()

        if(eventCorrelationId.equals(correlationId) &&  transExecValue.equals(received_date)) {
            softly.assertThat(avroRecordConsum.get("eventHeader.eventType").toString()).IsEqualTo(eventType)

            softly.assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(transExecValue)
            softly.assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(transExecValue)
            softly.assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(correlationId)
            softly.assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(correlationId)
            softly.assertThat(avroRecordConsum.get("eventHeader.eventSource").to String()).isEqualTo(eventType)
            softly.assertThat(avroRecordConsum.get("eventHeader.eventInitiatorDescription").to String()).isEqualTo(eventType)
            softly.assertThat(avroRecordConsum.get("eventHeader.eventInitiator").to String()).isEqualTo(eventType)
            softly.assertThat(avroRecordConsum.get("eventBody.messageContent").toString()).isEqualTo(textSMS)
            softly.assertThat(avroRecordConsum.get("eventBody.phoneNumber").toString()).isEqualTo(textSubjectExpected)
            softly.assertThat(applicationId).isEqualTo(applicationIdSMS)
            softly.assertThat(emailDB).isEqualTo(emailExpected)
            softly.assertThat(replyToDB).isEqualTo(replyToExpected)
            softly.assertThat(emailSubjectDB).isEqualTo(textSubjectExpected)
            softly.assertThat(emailBodyDB).isEqualTo(textBodyExpected)
        }
        long offset = record.offset()
        log.info("Consumed record at offset: " + offset)
        softly.assertAll()
    }
    conn.close()
}
log.info("Validations passed with success")



