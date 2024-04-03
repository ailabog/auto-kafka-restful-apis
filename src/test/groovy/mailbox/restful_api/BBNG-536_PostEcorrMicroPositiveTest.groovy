package mailbox.restful_api


import okhttp3.MediaType

//Define necessary variables for the Ecorr  Call

String base_url = ""
String middle_url = ""
String end_url = ""

String URL = base_url + middle_url + end_url
String username = ""
String password = ""


RequestBody  ecorrRequestBody = RequestBody.create(MediaType.parse("application/json"), '{"messageType":"email","sender":{"replyTo":"","alias":"","email":""},"messageFormat":"text","recipients":[{"recipientType":"to","email":""}],"applicationId":"","content":{"items":[{"templateType":"text","index":0,"text":"+12672133096","contentType":"emailSubject"},{"templateType":"text","index":1,"text":"Automated email sent with success today","contentType":"emailBody"}]}}')

OkHttpClient client = new OkHttpClient()
Request request_ecorr = new Request.Builder().url(URL).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(ecorrRequestBody).build()


Response response_ecorr = client.newCall(request_ecorr).execute()
String responseBody_ecorr= response_ecorr.body().string()
int statusCode_ecorr = response_ecorr.code()

log.info("Status code for Call:" + statusCode_ecorr)
log.info("Response for" + responseBody_ecorr.toString())