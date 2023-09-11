package com.mycompany.gzip.java.client;

import javax.jms.*;
import org.apache.qpid.jms.JmsConnectionFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.json.JSONObject;

public class Receiver {

    public static void main(String[] args) {
        String artemisAddress = "amqp://10.37.129.2:61616";
        String username = "admin";
        String password = "admin";
        String queueName = "test/java";

        try {
            JmsConnectionFactory connectionFactory = new JmsConnectionFactory(artemisAddress);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);

            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);

            MessageConsumer consumer = session.createConsumer(queue);

            while (true) {
                // Receive the message
                Message message = consumer.receive();

                // Start the timer
                long startTime = System.currentTimeMillis();

                if (message instanceof BytesMessage) {
                    BytesMessage bytesMessage = (BytesMessage) message;

                    // Read the compressed data
                    byte[] compressedData = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(compressedData);

                    // Print the size of the received data
                    System.out.println("Received data size: " + compressedData.length + " bytes");

                    // Gzip decompression
                    try ( ByteArrayInputStream compressedStream = new ByteArrayInputStream(compressedData);  GZIPInputStream gzipStream = new GZIPInputStream(compressedStream)) {

                        byte[] decompressedData = new byte[1024];
                        int length;
                        StringBuilder stringBuilder = new StringBuilder();

                        while ((length = gzipStream.read(decompressedData)) > 0) {
                            stringBuilder.append(new String(decompressedData, 0, length));
                        }

                        // Parse the JSON message
                        JSONObject jsonMessage = new JSONObject(stringBuilder.toString());

                        // End the timer
                        long endTime = System.currentTimeMillis();

                        long elapsedTime = endTime - startTime;
                        System.out.println("Sent JSON Message in " + elapsedTime + " ms");

                        // Process the JSON message
                        System.out.println("Received JSON Message:");
                        // System.out.println(jsonMessage.toString());

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
