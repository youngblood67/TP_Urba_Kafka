package org.cnam.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.cnam.kafka.producer.model.StationLocation;
import org.cnam.kafka.producer.model.StationLocationSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.client.RestTemplate;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ProducerApplication {
    private static final String URL_API_JCDECAUX ="https://api.jcdecaux.com/vls/v1/stations?apiKey=6655a9efa78e1190b723f7d124589513af17c6cb";
    private static final String KAFKA_BROKER_URL = "127.0.0.1:9092";
    private static final String TOPIC_NAME = "tp-topic";
    private static final String CLIENT_ID = "tp-consumer-1";
    private int counter = 0;

    public ProducerApplication() throws JsonProcessingException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_URL);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StationLocationSerializer.class.getName());

        KafkaProducer<String, StationLocation> producer = new KafkaProducer<String, StationLocation>(properties);
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {

            RestTemplateBuilder restTemplateBuilder = new RestTemplateBuilder();
            RestTemplate restTemplate = restTemplateBuilder.build();
            StationLocation[] stationLocations = restTemplate.getForObject(URL_API_JCDECAUX, StationLocation[].class);
            assert stationLocations != null;
            for(StationLocation stationLocation : stationLocations){
                counter++;
                producer.send(new ProducerRecord<String, StationLocation>(TOPIC_NAME, String.valueOf(counter), stationLocation),
                        (metadata, ex) -> {
                            System.out.println("PRODUCER : Sending message key => " + counter + " Value => " + stationLocation + " on Partition => " + metadata.partition() + " Offset => " + metadata.offset());
                        });

            }
            System.out.println("TOTAL STATIONS : " +  stationLocations.length);





        }, 1000, 30000, TimeUnit.MILLISECONDS);





    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

}
