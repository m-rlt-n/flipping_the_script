package org.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public class CaseDataToKafka {
    private static final String KAFKA_TOPIC = "county_cases";

    public static void main(String[] args) {
        // Configure Kafka Producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Try to run producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            String apptoken = System.getenv("COOK_CNTY_APP_TOKEN");
            // Set headers to accept JSON and include the app token
            String[] headers = {"Accept: application/json", "X-App-Token: " + apptoken};
            String url = "https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json";
            Random random = new Random();
            int offset = random.nextInt(5000);
            JSONObject jsonData  = HttpRequestHelper.fetchData(url, 1, offset, headers);
            System.out.println("DATA " + jsonData);
            DispositionData dispositionData = DispositionDataConverter.convertResponseToDispositionData(jsonData);
            assert dispositionData != null;
            System.out.println("Disposition Case ID: " + dispositionData.caseId);

                // Send to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, dispositionData.caseId, dispositionData.toJsonString());
                producer.send(record);
            } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
