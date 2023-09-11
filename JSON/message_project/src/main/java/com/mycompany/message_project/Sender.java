package com.mycompany.message_project;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import javax.jms.*;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.json.JSONObject;

public class Sender {

    public static void main(String[] args) {
        String artemisAddress = "amqp://10.37.129.2:61616";
        String username = "admin";
        String password = "admin";
        String queueName = "test/java";
        
        int sizeOFArray = 10000;

        try {
            JmsConnectionFactory connectionFactory = new JmsConnectionFactory(artemisAddress);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);

            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);

            MessageProducer producer = session.createProducer(queue);

            // Create a JSON representation of your data
            JSONObject jsonMessage = new JSONObject();

            // Create an int array from 1 to 100000
            int[] interestsArray = new int[sizeOFArray];
            for (int i = 0; i < sizeOFArray; i++) {
                interestsArray[i] = i + 1;
            }
            jsonMessage.put("intArray", interestsArray);

            // Create a float array from 1 to 10000
            float[] floatArray = new float[sizeOFArray];

            for (int i = 0; i < sizeOFArray; i++) {
                floatArray[i] = i + 1;
            }

            jsonMessage.put("floatArray", floatArray);

            // Create a string array from 1 to 100000
            String[] stringArray = new String[sizeOFArray];
            Random random = new Random();
            for (int i = 0; i < sizeOFArray; i++) {
                // Generate a random string of length 10
                StringBuilder randomString = new StringBuilder();
                for (int j = 0; j < 10; j++) {
                    char randomChar = (char) (random.nextInt(26) + 'a');
                    randomString.append(randomChar);
                }
                stringArray[i] = randomString.toString();
            }

            jsonMessage.put("stringArray", stringArray);

        
            // Start the timer
            long startTime = System.currentTimeMillis();

            // Convert JSON to byte array
            byte[] jsonDataBytes = jsonMessage.toString().getBytes();

            // Create a BytesMessage and set the byte array as its payload
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(jsonDataBytes);

            // Send the message
            producer.send(bytesMessage);

            // End the timer
            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            System.out.println("Sent JSON Message in " + elapsedTime + " ms");

            System.out.println("Sent JSON Message as Byte Array:");
            System.out.println(jsonMessage.toString());

            // Convert JSON to String
            String jsonData = jsonMessage.toString();

            // Specify the file path
            String filePath = "/Users/nagkim/NetBeansProjects/mavenproject2/message_project/json/data.json";

            // Create a FileWriter and write JSON data to the file
            try ( FileWriter fileWriter = new FileWriter(filePath)) {
                fileWriter.write(jsonData);
                System.out.println("JSON data written to file: " + filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
