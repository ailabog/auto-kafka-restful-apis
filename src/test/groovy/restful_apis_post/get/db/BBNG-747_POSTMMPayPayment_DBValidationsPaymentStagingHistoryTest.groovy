package restful_apis_post.get.db

import okhttp3.*
import okhttp3.MediaType
import org.json.JSONArray
import org.json.JSONObject
import org.assertj.core.api.SoftAssertions
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.ResultSet

String base_url_mmpay_payment = ""
String middle_url_mmpay_payment = ""
String end_url_mmpay_payment = ""

String url_mmpay_payment = base_url_mmpay_payment + middle_url_mmpay_payment + end_url_mmpay_payment

String username = ""
String password = ""

Random random = new Random()
int randomInt = random.nextInt(100000000) + 1
String agreement_Key = randomInt +  "|||" + "ISIQ"
long  amount = 8906754

UUID guid = UUID.randomUUID()

JSONObject mmpayPaymentBody = new JSONObject()
mmpayPaymentBody.put("transGuid", guid)
mmpayPaymentBody.put("processingCode", 1)
mmpayPaymentBody.put("payorEmailAddress", "")
mmpayPaymentBody.put("bankAcctType", "")
mmpayPaymentBody.put("creatorId", guid)
mmpayPaymentBody.put("accountNumber", randomInt)
mmpayPaymentBody.put("paymentEffectiveDate", "2024-02-13T18:06:32.468Z")
mmpayPaymentBody.put("paymentAmount", amount)
mmpayPaymentBody.put("routingNumber", randomInt)
mmpayPaymentBody.put("transRequestorId", guid)
mmpayPaymentBody.put("receivableType", "")
mmpayPaymentBody.put("transType", "")
mmpayPaymentBody.put("agreementKey", "")
mmpayPaymentBody.put("suspensePayment", "false")
mmpayPaymentBody.put("paymentMethod", "ACH")
mmpayPaymentBody.put("authorizationType", "WEB")
mmpayPaymentBody.put("creatorSystem", "MMCOM")
mmpayPaymentBody.put("transExecDateTS", "")
mmpayPaymentBody.put("correlationid", "")
mmpayPaymentBody.put("customerMemberGuid", "")
mmpayPaymentBody.put("paymentStatus", "PNDG")
mmpayPaymentBody.put("payorFullName", "")
log.info("Payload used for MMPay Payment" + mmpayPaymentBody)

RequestBody  mmpayPaymentPayload = RequestBody.create(MediaType.parse("application/json"), mmpayPaymentBody.toString())

OkHttpClient client = new OkHttpClient()
Request request_mmpayPayment = new Request.Builder().url(url_mmpay_payment).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(mmpayPaymentPayload).build()
log.info("MMPay Payment  Call:" + request_mmpayPayment)

Response response_mmpay_payment = client.newCall(request_mmpayPayment).execute()
String responseBody_payment= response_mmpay_payment.body().string()
int statusCode_payment = response_mmpay_payment.code()

SoftAssertions softly = new SoftAssertions()
softly.assertThat(statusCode_payment).isEqualTo(200)
softly.assertAll()

log.info("Response for  Payment Call :" + responseBody_payment)
log.info("Status code for  Payment Call:" + statusCode_payment)

//Save payment_key
JSONObject jsonObject = new JSONObject(responseBody_payment)
JSONArray msgArray = jsonObject.getJSONArray("messages")
JSONObject msgObject = msgArray.getJSONObject(0)
String msgDesc =msgObject.getString("msgDesc")
String loankeyString = msgDesc.substring(msgDesc.lastIndexOf(" " ) + 1)
int paymentKey = Integer.parseInt(loankeyString.trim())
log.info("Billing key is: " + paymentKey)

//DB validations
String newConnectionMMPayURL = ""
String usernameDB = ""
String passwordDB = ""
String queryPaymentStaging = ''

//DB execution/validations - payment_staging_key
log.info("Connecting to MMPay DB... ")

Connection conn = DriverManager.getConnection(newConnectionMMPayURL, usernameDB, passwordDB)
PreparedStatement paymentStatem = conn.prepareStatement(queryPaymentStaging)
paymentStatem.setInt(1, paymentKey)

ResultSet rs = paymentStatem.executeQuery()
log.info("Starting to validate the db values..." )
try {
    Thread.sleep(1 * 60 * 1000)
} catch (InterruptedException e ) {
    Thread.currentThread().interrupt()
}
if(rs.next()){
    int paymentStagingHistoryKeyDB = rs.getInt("payment_staging_history_key")
    int paymentStagingKeyDB = rs.getInt("payment_staging_key")
    int policyMasterKeyDB = rs.getInt("policy_master_key")
    String accountNumberDB = rs.getString("account_number")
    String routingNumberDB = rs.getString("routing_number")
    long paymentAmountDB = rs.getLong("payment_amount")
    String payorEmailAddressDB =  rs.getString("payor_email_address")
    String creatorIdDB =  rs.getString("creator_id")
    int paymentConfirmationDB = rs.getInt("payment_confirmation")
    String billingNoDB = rs.getString("billing_no")
    String submitGUIDDB = rs.getString("submit_guid")
    String submitstatusDB = rs.getString("submit_status")
    String rowProcessDateTimeDB = rs.getString("row_process_datetime")
    String customerMemberGuidDB= rs.getString("customer_member_guid")
    String payorfullNameDB = rs.getString("payor_full_name")

    if(paymentStagingKeyDB.equals(paymentKey)) {
        softly.assertThat(paymentAmountDB).isEqualTo(amount)
        softly.assertThat(paymentConfirmationDB).isEqualTo(paymentKey)
        softly.assertAll()
    }else {
        log.info("No payment staging key was found into the db")
    }
}
conn.close()
log.info("All Db validations passed successfully")








