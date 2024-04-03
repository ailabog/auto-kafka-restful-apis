package restful_api_get_db

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

String base_url_mmpay_receivables = ""
String middle_url_mmpay_receivables = ""
String end_url_mmpay_receivables = ""
String final_url_mmpay_receivables = ""

String url_mmpay_receivables_get = base_url_mmpay_receivables + middle_url_mmpay_receivables + end_url_mmpay_receivables + final_url_mmpay_receivables
String username = ""
String password = ""

//GET MMPay Receivables
log.info("Creating GET MMPay Receivables with ..." + url_mmpay_receivables_get)
OkHttpClient client = new OkHttpClient()
Request request_mmpay_receivables_get = new Request.Builder().url(url_mmpay_receivables_get).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").build()

log.info("Calling the GET MMPay Receivables ...")
Response response_mmpay_receivables_get = client.newCall(request_mmpay_receivables_get).execute()
String responseBody_receivables_get= response_mmpay_receivables_get.body().string()
int statusCode_receivables_get = response_mmpay_receivables_get.code()

SoftAssertions softly = new SoftAssertions()

softly.assertThat(statusCode_receivables_get).isEqualTo(200)
softly.assertAll()

log.info("Response for GET MMPay Receivables :" + responseBody_receivables_get)
log.info("Status code for GET MMPay Receivables :" + statusCode_receivables_get)

//Parse Get Receivables  response
JSONObject getPymntRsp = new JSONObject(responseBody_receivables_get)
String transTypegetPymntRsp = getPymntRsp.getString("transType")
String transExecDateTSPymntRsp = getPymntRsp.getString("transExecDateTS")

JSONArray messagArray = getPymntRsp.getJSONArray("messages")
JSONObject messageObjectgetPymntRsp = messagArray.getJSONObject(0)
String messageType = messageObjectgetPymntRsp.getString("msgType")
String messageDesc = messageObjectgetPymntRsp.getString("msgDesc")

JSONArray receivableBillsArray = getPymntRsp.getJSONArray("receivableBills")
JSONObject receivablesBillsObject = receivableBillsArray.getJSONObject(0)

String agreementKeyGetRsp = receivablesBillsObject.getString("agreementKey")
String policyMasterKeyGetRsp = receivablesBillsObject.getString("policyMasterKey")
int policyMasterKeyGetRspInt = Integer.parseInt(policyMasterKeyGetRsp)
String billingKeyGetRsp = receivablesBillsObject.getString("billingKey")
int billingKeyGetRspInt = Integer.parseInt(billingKeyGetRsp)
String typeOfBillGetRsp = receivablesBillsObject.getString("typeOfBill")
String paymentFrequencyCodeGetRsp = receivablesBillsObject.getString("paymentFrequencyCode")
String paymentFrequencyKeyGetRsp = receivablesBillsObject.getString("paymentFrequencyKey")
String billNoGetRsp = receivablesBillsObject.getString("billNo")
String amountDueGetRsp = receivablesBillsObject.getString("amountDue")
String minimumPaymentGetRsp = receivablesBillsObject.getString("minimumPayment")
String maximumPaymentGetRsp = receivablesBillsObject.getString("maximumPayment")
String receivableStatusCodeGetRsp = receivablesBillsObject.getString("receivableStatusCode")
String receivableStatusKeyGetRsp = receivablesBillsObject.getString("receivableStatusKey")

String newConnectionURL = ""
String usernameDB = ""
String passwordDB = ""
String queryBilling = ''

//DB Validations
log.info("Connecting to DB... ")
Connection conn = DriverManager.getConnection(newConnectionURL, usernameDB, passwordDB)
PreparedStatement paymentStatem = conn.prepareStatement(queryBilling)
paymentStatem.setInt(1, billingKeyGetRspInt)

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
    log.info("Master policy master key/billing key found into the db so the validations are required: " + policyMasterKeyDB + " " + billingKeyDB )

    if(policyMasterKeyDB.equals(policyMasterKeyGetRspInt)) {
        softly.assertThat(maxPaymentDB).isEqualTo(maximumPaymentGetRsp)
        softly.assertThat(amountDueDB).isEqualTo(amountDueGetRsp)
        softly.assertThat(policyMasterKeyDB).isEqualTo(policyMasterKeyGetRspInt)
        softly.assertAll()
    }
}
conn.close()
log.info("Db validations passed successfully")

