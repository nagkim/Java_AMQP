/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.message_project;

import java.io.FileWriter;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.json.JSONObject;

import javax.jms.*;

/**
 *
 * @author nagkim
 */
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

                    // Read the byte array from the message
                    byte[] jsonDataBytes = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(jsonDataBytes);

                    // Convert the byte array to JSON string
                    String jsonData = new String(jsonDataBytes);

                    // Parse the JSON message
                    JSONObject jsonMessage = new JSONObject(jsonData);
                    
                    // Print the size of the received data
                    System.out.println("Received data size: " + jsonDataBytes.length + " bytes");

                    long endTime = System.currentTimeMillis();

                    long elapsedTime = endTime - startTime;
                    System.out.println("Sent JSON Message in " + elapsedTime + " ms");

                    // Save the JSON data to a file
                    String filePath = "/Users/nagkim/NetBeansProjects/mavenproject2/message_project/json/received_data.json";
                    try ( FileWriter fileWriter = new FileWriter(filePath)) {
                        fileWriter.write(jsonMessage.toString());
                        System.out.println("Received JSON data written to file: " + filePath);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // You can perform your processing logic here
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
