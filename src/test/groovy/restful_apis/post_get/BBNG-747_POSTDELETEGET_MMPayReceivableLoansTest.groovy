package restful_apis.post_get

import okhttp3.*
import okhttp3.MediaType
import org.json.JSONArray
import org.json.JSONObject
import java.util.Random
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.assertj.core.api.SoftAssertions

String base_url_mmpay_receivables = ""
String middle_url_mmpay_receivables = ""
String end_url_mmpay_receivables = ""

Random random = new Random()
int randomInt = random.nextInt(10000000)
String agreement_Key = randomInt.toString() +  "|||" + "OPM"

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

String url_mmpay_receivables = base_url_mmpay_receivables + middle_url_mmpay_receivables + end_url_mmpay_receivables

String username = ""
String password = ""

log.info("Creating  POST Receivables  Call ..." + url_mmpay_receivables)

JSONObject requestBodyJson = new JSONObject()
requestBodyJson.put("agreementKey", agreement_Key)
requestBodyJson.put("loanId", "")
requestBodyJson.put("loanAmount", 500000)
requestBodyJson.put("quoteDate", currentDateComplete)

RequestBody  mmpayReceivablesBody = RequestBody.create(MediaType.parse("application/json"), requestBodyJson.toString())

OkHttpClient client = new OkHttpClient()
Request request_mmpayReceivables = new Request.Builder().url(url_mmpay_receivables).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(mmpayReceivablesBody).build()
log.info("MMPay Receivables  Call:" + request_mmpayReceivables)

Response response_mmpay_receivables = client.newCall(request_mmpayReceivables).execute()
String responseBody_receivables= response_mmpay_receivables.body().string()

log.info("response:" + responseBody_receivables)
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
int loankey = Integer.parseInt(loankeyString.trim())

//GET MMPay Receivables
String url_mmpay_receivables_get = url_mmpay_receivables + agreement_Key
log.info("Creating GET MMPay Receivables with ..." + url_mmpay_receivables_get)

Request request_mmpay_receivables_get = new Request.Builder().url(url_mmpay_receivables_get).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").build()

log.info("Calling the GET MMPay Receivables ...")
Response response_mmpay_receivables_get = client.newCall(request_mmpay_receivables_get).execute()
String responseBody_receivables_get= response_mmpay_receivables_get.body().string()
int statusCode_receivables_get = response_mmpay_receivables.code()

softly.assertThat(statusCode_receivables).isEqualTo(200)
softly.assertAll()

log.info("Response for GET MMPay Receivables :" + responseBody_receivables_get)
log.info("Status code for GET MMPay Receivables :" + statusCode_receivables_get)

//DELETE MMPay Receivables
String url_mmpay_receivables_delete =  url_mmpay_receivables + loankey
log.info("Creating  POST MMPay Receivables  Call ..." + url_mmpay_receivables)

Request request_mmpayReceivables_delete = new Request.Builder().url(url_mmpay_receivables_delete).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").delete().build()
log.info("MMPay Receivables  Call:" + request_mmpayReceivables_delete)

Response response_mmpay_receivables_delete = client.newCall(request_mmpayReceivables_delete).execute()
String responseBody_receivables_delete= response_mmpay_receivables_delete.body().string()
int statusCode_receivables_delete = response_mmpay_receivables_delete.code()

softly.assertThat(statusCode_receivables).isEqualTo(200)
softly.assertAll()

log.info("Response for DELETE MMPay Receivables Call :" + responseBody_receivables)
log.info("Status code for DELETE MMPay Receivables Call:" + statusCode_receivables)

