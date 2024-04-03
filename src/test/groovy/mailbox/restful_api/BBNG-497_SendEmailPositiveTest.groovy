package mailbox.restful_api

import org.apache.commons.mail.DefaultAuthenticator
import org.apache.commons.mail.Email
import org.apache.commons.mail.EmailException
import org.apache.commons.mail.HtmlEmail

try {
    Email email = new HtmlEmail()
    email.setHostName("")
    email.setSmtpPort(465)

    email.setSSLOnConnect(true)
    email.setSslSmtpPort("465")

    email.setAuthenticator(new DefaultAuthenticator("", ""))
    //email.setStartTLSEnabled(true)

    email.getMailSession().getProperties().setProperty("mail.smtp.socketFactory.class",   "javax.net.ssl.SSLSocketFactory")
    email.getMailSession().getProperties().setProperty("mail.smtp.auth", "true")
    email.getMailSession().getProperties().setProperty("mail.smtp.socketFactory.port", 465)

    email.setFrom("")
    email.setSubject("Subject of the email")
    email.setMsg("Content of the email")
    email.addTo("l")

    email.send()
    log.info("Email was sent successfully")
} catch (EmailException e) {
    log.info("Error sending email" + e.getMessage())
}
