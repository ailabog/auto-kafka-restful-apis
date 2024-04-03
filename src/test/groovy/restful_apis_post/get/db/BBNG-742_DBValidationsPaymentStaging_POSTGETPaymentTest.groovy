package restful_apis_post.get.db


import java.util.*
import java.text.DateFormat
import java.text.SimpleDateFormat
import okhttp3.*
import okhttp3.MediaType
import org.json.JSONArray
import org.json.JSONObject
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.ResultSet
import org.assertj.core.api.SoftAssertions
import java.util.Random
import org.slf4j.*
import groovy.json.JsonBuilder
import java.text.DateFormat
import java.text.SimpleDateFormat

//POST MMPay Payment
String base_url = ""
String middle_url = ""
String end_url = ""

String url = base_url + middle_url + end_url

String username = ""
String password = ""

Random random = new Random()
int randomInt = random.nextInt(1000000) + 1
String agreement_Key = randomInt +  "|||" + "ISIQ"
String paymentAmount = "999999"
String receivableType = "PREM"

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

SimpleDateFormat dateT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
dateT.setTimeZone(TimeZone.getTimeZone("GMT-5"))
String formattedDate = dateT.format(now)

//Create paylod for the request
JSONObject body = new JSONObject()
body.put("receivableType", receivableType)
body.put("agreementKey", agreement_Key)
body.put("processingCode", 1)
body.put("paymentAmount", paymentAmount)
log.info("Payload used for MMPay Payment" + body)

RequestBody  payload = RequestBody.create(MediaType.parse("application/json"), body.toString())

OkHttpClient client = new OkHttpClient()
Request request = new Request.Builder().url(url).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(mmpayPaymentPayload).build()

Response response = client.newCall(request).execute()
String responseBody= response.body().string()
int statusCode = response.code()

SoftAssertions softly = new SoftAssertions()

softly.assertThat(statusCode).isEqualTo(200)
softly.assertAll()

log.info("Response for Payment Call :" + responseBody)
log.info("Status code for Payment Call:" + statusCode)

//GET MMPay Payment
String url_get = base_url + middle_url + end_url + agreement_Key

Request request_get = new Request.Builder().url(url).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").build()
log.info("Calling the GET payment ...")

Response response_get = client.newCall(request_get).execute()
String responseBody_get= response_get.body().string()
int statusCode_get = response_get.code()

softly.assertThat(statusCode_payment_get).isEqualTo(200)
softly.assertAll()

log.info("Response for GET MMPay payment :" + responseBody_get)
log.info("Status code for GET MMPay payment :" + statusCode_get)

//Parse Get Payment response
JSONObject getPymntRsp = new JSONObject(responseBody_get)
String transTypegetPymntRsp = getPymntRsp.getString("transType")
String transExecDateTSPymntRsp = getPymntRsp.getString("transExecDateTS")

JSONArray messagArray = getPymntRsp.getJSONArray("messages")
JSONObject messageObjectgetPymntRsp = messagArray.getJSONObject(0)
String messageType = messageObjectgetPymntRsp.getString("msgType")
String messageDesc = messageObjectgetPymntRsp.getString("msgDesc")

JSONArray paymentsArray = getPymntRsp.getJSONArray("payments")
JSONObject paymentsObject = paymentsArray.getJSONObject(0)
String agreementKeyGetRsp = paymentsObject.getString("agreementKey")
String paymentStagingKeyGetRsp = paymentsObject.getString("paymentStagingKey")
int paymentStagingKeyGetRspInt = Integer.parseInt(paymentStagingKeyGetRsp)
String policyMasterKeyGetRsp = paymentsObject.getString("policyMasterKey")
int policyMasterKeyGetRspInt = Integer.parseInt(policyMasterKeyGetRsp)
String paymentAmountGetRsp = paymentsObject.getString("paymentAmount")
String receivableTypeGetRsp = paymentsObject.getString("receivableType")
String paymentConfirmationGetRsp = paymentsObject.getString("paymentConfirmation")
String createdDtmGetRsp = paymentsObject.getString("createdDtm")

//PostgreSQL variables
String newConnectionMMPayURL = ""
String usernameDB = ""
String passwordDB = ""
String queryPaymentStaging = ''

//DB execution/validations
log.info("Connecting to DB... ")

Connection conn = DriverManager.getConnection(newConnectionMMPayURL, usernameDB, passwordDB)
PreparedStatement paymentStatem = conn.prepareStatement(queryPaymentStaging)
paymentStatem.setInt(1, policyMasterKeyGetRspInt)

ResultSet rs = paymentStatem.executeQuery()
log.info("Starting to validate the db values..." )

if(rs.next()) {
    int paymentStagingKeyDB = rs.getInt("payment_staging_key")
    int policyMasterKeyDB = rs.getInt("policy_master_key")
    String paymentAmountDB = rs.getString("payment_amount")
    String routingNumberDB = rs.getString("routing_number")
    String accountNumberDB = rs.getString("account_number")
    String paymentScheduleKeyDB = rs.getString("payment_schedule_key")
    String policyDueDtDB = rs.getString("policy_due_dt")
    String bankAcctTypeKeyDB = rs.getString("bank_acct_type_key")
    String billingNoDB = rs.getString("billing_no")
    String paymentConfirmationDB = rs.getString("payment_confirmation")
    String submitGUIDDB = rs.getString("submit_guid")
    String submitstatusDB = rs.getString("submit_status")
    String rowProcessDateTimeDB = rs.getString("row_process_datetime")

    if(policyMasterKeyDB.equals(policyMasterKeyGetRspInt)) {
        log.info("Master policy master  key is found into the db so the validations are required:" + policyMasterKeyDB)
        softly.assertThat(paymentAmountDB).isEqualTo(paymentAmountGetRsp)
        softly.assertThat(paymentStagingKeyDB).isEqualTo(paymentStagingKeyGetRspInt)
        softly.assertThat(paymentConfirmationDB).isEqualTo(paymentConfirmationGetRsp)
        softly.assertAll()
    }
}
conn.close()
log.info("Db validations passed successfully")


