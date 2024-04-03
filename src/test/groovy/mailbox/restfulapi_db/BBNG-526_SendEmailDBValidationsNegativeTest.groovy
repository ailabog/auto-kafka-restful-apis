package mailbox.restfulapi_db


import okhttp3.MediaType
import org.json.JSONArray
import org.json.JSONObject
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.ResultSet
import org.assertj.core.api.SoftAssertions

//Define necessary variables for the Ecorr  Call

String base_url_ecorr = ""
String middle_url_ecorr = ""
String end_url_ecorr = ""

String URL_ECORR = base_url_ecorr + middle_url_ecorr + end_url_ecorr
String username = ""
String password = ""

//Define ecorr db
String connectionUrl = ""
String usernameDB = ""
String passwordDB = ""
String querySMSCount = ''
String queryEmailCount = ''
String applicationIdExpected = ""

log.info("Creating  Call ..." + URL_ECORR)

RequestBody  ecorrRequestBody = RequestBody.create(MediaType.parse("application/json"), '{"messageType":"email","sender":{"replyTo":"","alias":"","email":""},"messageFormat":"text/plain","recipients":[{"recipientType":"to","email":""}],"applicationId":"","content":{"items":[{"templateType":"text","index":0,"text":"+40727890123","contentType":"emailSubject"},{"templateType":"text","index":1,"text":"Acesta este un email automat." ,"contentType":"emailBody"}]}}')

OkHttpClient client = new OkHttpClient()
Request request_ecorr = new Request.Builder().url(URL_ECORR).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(ecorrRequestBody).build()
log.info("Call:" + request_ecorr)

Response response_ecorr = client.newCall(request_ecorr).execute()
String responseBody_ecorr= response_ecorr.body().string()
int statusCode_ecorr = response_ecorr.code()

log.info("Status code for Call:" + statusCode_ecorr)
log.info("Response for:" + responseBody_ecorr.toString())

//Extract values for transExecDateTS/msgTblIdent from response message micro call
JSONObject jsonSendEmailObject = new JSONObject(responseBody_ecorr)
String transExecValue = jsonSendEmailObject.getString("transExecDateTS")
String msgTblIdent = jsonSendEmailObject.getString("msgTblIdent")

//Extracting fields from the request payload for comparison
JSONObject requestPayloadJson = new JSONObject('{"messageType":"email","sender":{"replyTo":","alias":"Aila Bogasieru","email":"abogasieru24@massmutual.com"},"messageFormat":"text","recipients":[{"recipientType":"to","email":""}],"applicationId":"","content":{"items":[{"templateType":"text","index":0,"text":"+40727890123","contentType":"emailSubject"},{"templateType":"text","index":1,"text":"Acesta este un email automat.","contentType":"emailBody"}]}}')

String emailExpected = requestPayloadJson.getJSONObject("sender").getString("email")
String replyToExpected = requestPayloadJson.getJSONObject("sender").getString("replyTo")
String textSubjectExpected = requestPayloadJson.getJSONObject("content").getJSONArray("items").getJSONObject(0).getString("text")
String textBodyExpected = requestPayloadJson.getJSONObject("content").getJSONArray("items").getJSONObject(1).getString("text")

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

if(emailCount == 1 && smsCount == 0 ){
    log.info("Ony one email record found into the db")
} else {
    log.info("Something went wrong")
}

if(rsEmail.next() &&(! rsSMS.next())) {

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

    if (transExecValue.equals(received_date)) {

        softly.assertThat(sourceDB).isEqualTo("")
        softly.assertThat(typeDB).isEqualTo("")
        softly.assertThat(applicationId).isEqualTo(applicationIdExpected)
        softly.assertThat(emailDB).isEqualTo(emailExpected)
        softly.assertThat(replyToDB).isEqualTo(replyToExpected)

        softly.assertThat(emailSubjectDB).isEqualTo(textSubjectExpected)
        softly.assertThat(emailBodyDB).isEqualTo(textBodyExpected)
        softly.assertAll()
    }
    log.info("Source from db:" +sourceDB+ " " + "Type from db is:" +typeDB+ "  "+"ApplicationID is:"+applicationId+ " " +"Email subject:" +emailSubjectDB+ " " +"Email body is:"+emailBodyDB)
}

conn.close()
log.info("Validating the DB fields passed with success")
