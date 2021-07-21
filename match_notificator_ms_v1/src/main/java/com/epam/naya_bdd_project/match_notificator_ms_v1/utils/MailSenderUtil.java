package com.epam.naya_bdd_project.match_notificator_ms_v1.utils;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class MailSenderUtil {
    //https://netcorecloud.com/tutorials/send-email-in-java-using-gmail-smtp/

    private static Session getSession(String fromAddr, String fromPass) {

        // Assuming you are sending email from through gmails smtp
        String host = "smtp.gmail.com";

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.put("mail.smtp.host", host);
        properties.put("mail.smtp.port", "465");
        properties.put("mail.smtp.ssl.enable", "true");
        properties.put("mail.smtp.auth", "true");


        // Get the Session object.// and pass username and password
        Session session = Session.getInstance(properties, new javax.mail.Authenticator() {

            protected PasswordAuthentication getPasswordAuthentication() {

                return new PasswordAuthentication(fromAddr, fromPass);

            }

        });

        // Used to debug SMTP issues
        session.setDebug(true);

        return session;
    }

    public static boolean send(String toAddr,String fromAddr, String fromPass, String subject, String messageBody)
    {
        // Recipient's email ID needs to be mentioned.
        //String to = "fromaddress@gmail.com";

        // Sender's email ID needs to be mentioned
        //String from = "toaddress@gmail.com";

        Session session = getSession(fromAddr,fromPass);

        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(fromAddr));

            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(toAddr));

            // Set Subject: header field
            message.setSubject(subject);

            // Now set the actual message
            message.setText(messageBody);

            System.out.println("sending...");
            // Send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
            return true;
        } catch (MessagingException mex) {
            mex.printStackTrace();
            return false;
        }
    }
}


