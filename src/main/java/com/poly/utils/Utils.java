package com.poly.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.*;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

public class Utils implements Serializable {

    String toEmail;
    String fromEmail;
    String subject;
    String body;
    private static Logger log = LogManager.getLogger(Utils.class);

    public Utils(String toEmail, String fromEmail, String subject, String body) {
        this.toEmail = toEmail;
        this.fromEmail = fromEmail;
        this.subject = subject;
        this.body = body;

    }

    public Utils() {

    }

    public void sendEMail() {
        AmazonSimpleEmailService client =
                AmazonSimpleEmailServiceClientBuilder.standard()
                        .withRegion(Regions.US_WEST_2)
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();
        SendEmailRequest request = new SendEmailRequest()
                .withDestination(
                        new Destination().withToAddresses(Arrays.asList(toEmail.split(","))))
                .withMessage(new Message()
                        .withBody(new Body()
                                .withText(new Content()
                                        .withCharset("UTF-8").withData(body)))
                        .withSubject(new Content()
                                .withCharset("UTF-8").withData(subject)))
                .withSource(fromEmail);
        client.sendEmail(request);
        log.info("Email sent to:" + toEmail);
    }

    public ByteArrayOutputStream writestreamGZIP(String inputdata) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream(50000000);
        try (GZIPOutputStream gzip = new GZIPOutputStream(out);
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(gzip, "UTF-8"), 1024)) {
            bw.write(inputdata);
        } catch (Exception e) {
            System.out.println(e.toString());

        }
        return out;
    }

    public ByteArrayOutputStream writestream(String inputdata) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream(50000000);
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"), 1024)) {
            bw.write(inputdata);
        } catch (Exception e) {
            System.out.println(e.toString());

        }
        return out;
    }

    public String lineReplace(String input) {
        return new String(input.trim().replaceAll("[\r\n]+", " "));
    }

    public String getSSMParam(String key) {
        AWSSimpleSystemsManagement cli = AWSSimpleSystemsManagementClientBuilder
                .defaultClient();
        GetParameterRequest req = new GetParameterRequest();
        req.withWithDecryption(true).withName(key);
        GetParameterResult result = cli.getParameter(req);
        return result.getParameter().getValue();
    }


}
