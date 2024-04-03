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
import java.sql.Timestamp

//Define necessary variables for  POST Receivables
String base_url_mmpay_receivables = " "
String middle_url_mmpay_receivables = ""
String end_url_mmpay_receivables = ""

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

Random random = new Random()
int randomInt = random.nextInt(10000) + 1
String agreement_Key = randomInt +  "|||" + "VUL"

String url_mmpay_receivables = base_url_mmpay_receivables + middle_url_mmpay_receivables + end_url_mmpay_receivables

String username = ""
String password = ""
int amount = 333333
String emailAddress = ""

log.info("Creating  POST Receivables  Call ..." + url_mmpay_receivables)

JSONObject requestBodyJson = new JSONObject()
requestBodyJson.put("agreementKey", agreement_Key)
requestBodyJson.put("loanId", emailAddress)
requestBodyJson.put("loanAmount", amount)
requestBodyJson.put("quoteDate", currentDateComplete)
log.info("Payload" + requestBodyJson)

RequestBody  mmpayReceivablesBody = RequestBody.create(MediaType.parse("application/json"), requestBodyJson.toString())

OkHttpClient client = new OkHttpClient()
Request request_mmpayReceivables = new Request.Builder().url(url_mmpay_receivables).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(mmpayReceivablesBody).build()
log.info("MMPay Receivables  Call:" + request_mmpayReceivables)

Response response_mmpay_receivables = client.newCall(request_mmpayReceivables).execute()
String responseBody_receivables= response_mmpay_receivables.body().string()
int statusCode_receivables = response_mmpay_receivables.code()

SoftAssertions softly = new SoftAssertions()

softly.assertThat(statusCode_receivables).isEqualTo(200)
softly.assertAll()

log.info("Response for MMPay Receivables Call :" + responseBody_receivables)
log.info("Status code for MMPay Receivables Call:" + statusCode_receivables)

//Save loan key
JSONObject jsonObject = new JSONObject(responseBody_receivables)
JSONArray msgArray = jsonObject.getJSONArray("messages")
JSONObject msgObject = msgArray.getJSONObject(0)
String msgDesc =msgObject.getString("msgDesc")
String loankeyString = msgDesc.substring(msgDesc.lastIndexOf(" ") + 1)
int loanKeyInt = Integer.parseInt(loankeyString.trim())
log.info("Loan key from POST Receivable call is: " + loanKeyInt)

//GET MMPay Receivables
String url_mmpay_receivables_get = url_mmpay_receivables + agreement_Key
log.info("Creating GET MMPay Receivables with ..." + url_mmpay_receivables_get)

Request request_mmpay_receivables_get = new Request.Builder().url(url_mmpay_receivables_get).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").build()

log.info("Calling the GET Receivables ...")
Response response_mmpay_receivables_get = client.newCall(request_mmpay_receivables_get).execute()
String responseBody_receivables_get= response_mmpay_receivables_get.body().string()
int statusCode_receivables_get = response_mmpay_receivables.code()

log.info("Response for GET Receivables :" + responseBody_receivables_get)
log.info("Status code for GET Receivables :" + statusCode_receivables_get)

softly.assertThat(statusCode_receivables_get).isEqualTo(200)
softly.assertAll()

//Parse Get Receivables  response
JSONObject getPymntRsp = new JSONObject(responseBody_receivables_get)
String transTypegetPymntRsp = getPymntRsp.getString("transType")
String transExecDateTSPymntRsp = getPymntRsp.getString("transExecDateTS")

JSONArray messagArray = getPymntRsp.getJSONArray("messages")
JSONObject messageObjectgetPymntRsp = messagArray.getJSONObject(0)
String messageType = messageObjectgetPymntRsp.getString("msgType")
String messageDesc = messageObjectgetPymntRsp.getString("msgDesc")

JSONArray receivableBillsArray = getPymntRsp.getJSONArray("receivableLoans")
JSONObject receivablesBillsObject = receivableBillsArray.getJSONObject(0)

String agreementKeyGetRsp = receivablesBillsObject.getString("agreementKey")
String policyMasterKeyGetRsp = receivablesBillsObject.getString("policyMasterKey")
int policyMasterKeyGetRspInt = Integer.parseInt(policyMasterKeyGetRsp)
//String billingKeyGetRsp = receivablesBillsObject.getString("billingKey")
//String typeOfBillGetRsp = receivablesBillsObject.getString("typeOfBill")
//String paymentFrequencyCodeGetRsp = receivablesBillsObject.getString("paymentFrequencyCode")
//String paymentFrequencyKeyGetRsp = receivablesBillsObject.getString("paymentFrequencyKey")
//String billNoGetRsp = receivablesBillsObject.getString("billNo")
int loanAmountGetRsp = receivablesBillsObject.getInt("loanAmount")
String loanIdGetRsp = receivablesBillsObject.getString("loanId")
int outstandingLoanKey = receivablesBillsObject.getInt("outstandingLoanKey")
//String minimumPaymentGetRsp = receivablesBillsObject.getString("minimumPayment")
//String receivableStatusCodeGetRsp = receivablesBillsObject.getString("receivableStatusCode")
//String receivableStatusKeyGetRsp = receivablesBillsObject.getString("receivableStatusKey")

//PostgreSQL variables
String newConnectionMMPayURL = ""
String usernameDB = ""
String passwordDB = ""
String queryOutstandingLoan = ''

//DB validations
Connection conn = DriverManager.getConnection(newConnectionMMPayURL, usernameDB, passwordDB)
PreparedStatement statement = conn.prepareStatement(queryOutstandingLoan)
statement.setInt(1, outstandingLoanKey)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    int  outstandingLoanKeyDB = rs.getInt("outstanding_loan_key")
    int policyMasterKeyDB = rs.getInt("policy_master_key")
    int loanAmtDB = rs.getInt("loan_amt")
    String rowProcessDatetimeDB = rs.getString("row_process_datetime")
    String quoteDtDB = rs.getString("quote_dt")
    String loandIdDB = rs.getString("loan_id")

    if (policyMasterKeyDB.equals(policyMasterKeyGetRspInt)) {

        softly.assertThat(outstandingLoanKey).isEqualTo(outstandingLoanKey)
        softly.assertThat(policyMasterKeyDB).isEqualTo(policyMasterKeyGetRspInt)
        softly.assertThat(loanAmtDB).isEqualTo(loanAmountGetRsp)
        softly.assertThat(loandIdDB).isEqualTo(loanIdGetRsp)
        softly.assertAll()
    } else {
        log.info("No row found in db for policy master key: " + policyMasterKeyGetRspInt)
    }
}
conn.close()








