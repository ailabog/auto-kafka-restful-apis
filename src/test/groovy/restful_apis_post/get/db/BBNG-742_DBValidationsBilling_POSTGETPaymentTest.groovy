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
String base_url_mmpay_payment = ""
String middle_url_mmpay_payment = ""
String end_url_mmpay_payment = ""

String url_mmpay_payment = base_url_mmpay_payment + middle_url_mmpay_payment + end_url_mmpay_payment

String username = ""
String password = ""

Random random = new Random()
int randomInt = random.nextInt(1000000) + 1
String agreement_Key = randomInt +  "|||" + "ISIQ"
String paymentAmount = "999999"
String receivableType = ""

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

SimpleDateFormat dateT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
dateT.setTimeZone(TimeZone.getTimeZone("GMT-5"))
String formattedDate = dateT.format(now)

//Create paylod for the request
JSONObject mmpayPaymentBody = new JSONObject()
mmpayPaymentBody.put("receivableType", receivableType)
mmpayPaymentBody.put("agreementKey", agreement_Key)
mmpayPaymentBody.put("processingCode", 1)
mmpayPaymentBody.put("paymentAmount", paymentAmount)
log.info("Payload used for MMPay Payment" + mmpayPaymentBody)

RequestBody  mmpayPaymentPayload = RequestBody.create(MediaType.parse("application/json"), mmpayPaymentBody.toString())

OkHttpClient client = new OkHttpClient()
Request request_mmpayPayment = new Request.Builder().url(url_mmpay_payment).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(mmpayPaymentPayload).build()
log.info("Payment  Call:" + request_mmpayPayment)

Response response_mmpay_payment = client.newCall(request_mmpayPayment).execute()
String responseBody_payment= response_mmpay_payment.body().string()
int statusCode_payment = response_mmpay_payment.code()

SoftAssertions softly = new SoftAssertions()

softly.assertThat(statusCode_payment).isEqualTo(200)
softly.assertAll()

log.info("Response for Payment Call :" + responseBody_payment)
log.info("Status code for Payment Call:" + statusCode_payment)

//GET MMPay Payment
String url_mmpay_payment_get = base_url_mmpay_payment + middle_url_mmpay_payment + end_url_mmpay_payment + agreement_Key

log.info("Creating GET payment with ..." + url_mmpay_payment_get)

Request request_mmpay_payment_get = new Request.Builder().url(url_mmpay_payment_get).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").build()
log.info("Calling the GET  payment ...")

Response response_mmpay_payment_get = client.newCall(request_mmpay_payment_get).execute()
String responseBody_payment_get= response_mmpay_payment_get.body().string()
int statusCode_payment_get = response_mmpay_payment_get.code()

softly.assertThat(statusCode_payment_get).isEqualTo(200)
softly.assertAll()

log.info("Response for GET  payment :" + responseBody_payment_get)
log.info("Status code for GET  payment :" + statusCode_payment_get)

//Parse Get Payment response
JSONObject getPymntRsp = new JSONObject(responseBody_payment_get)
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
String queryBilling = ''

//DB Validations
log.info("Connecting to MMPay DB... ")
Connection conn = DriverManager.getConnection(newConnectionMMPayURL, usernameDB, passwordDB)
PreparedStatement paymentStatem = conn.prepareStatement(queryBilling)
paymentStatem.setInt(1, policyMasterKeyGetRspInt)

ResultSet rs = paymentStatem.executeQuery()
log.info("Starting to validate the db values..." )

if(rs.next()) {
    int billingKeyDB = rs.getInt("billing_key")
    int policyMasterKeyDB = rs.getInt("policy_master_key")
    String billingNoDB = rs.getString("bill_no")
    String amountDueDB = rs.getString("amount_due")
    String maxPaymentDB = rs.getString("maximum_payment")
    String typeOfBillDB = rs.getString("type_of_bill")
    String rowProcessDateTime = rs.getString("row_process_datetime")
    log.info("Master policy master key is found into the db so the validations are required" + policyMasterKeyDB)

    if(policyMasterKeyDB.equals(policyMasterGetPymntInt)) {
        log.info("Master policy master  key is found into the db so the validations are required" + policyMasterKeyDB)
        softly.assertThat(creatorDtmDB).isEqualTo(currentDateComplete)
        softly.assertThat(maxPaymentDB).isEqualTo(amount)
        softly.assertThat(amountDueDB).isEqualTo(amount)
        softly.assertThat(policyMasterKeyDB).isEqualTo(policyMasterKeyGetRsp)
        softly.assertThat(eventresultCdDB).isEqualTo("BEM-PAYMENT-SUBMITTED")
        softly.assertThat(eventresultCdDB).isEqualTo("SUCCESS")
        softly.assertAll()
    }else {
        log.info("Something went wrong")
    }
    conn.close()
}
log.info("Db validations passed successfully")
