package org.cnam.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.cnam.kafka.consumer.model.StationLocation;
import org.cnam.kafka.consumer.model.StationLocationDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication

public class ConsumerApplication {
    private static final String KAFKA_BROKER_URL = "127.0.0.1:9092";
    private static final String TOPIC_NAME = "tp-topic";
    private static final String GROUP_ID = "tp-group";
    private List<StationLocation> stationLocationList = new ArrayList<>();

    public ConsumerApplication() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_URL);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StationLocationDeserializer.class.getName());

        KafkaConsumer<String, StationLocation> consumer = new KafkaConsumer<String, StationLocation>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            ConsumerRecords<String, StationLocation> consumerRecords = consumer.poll(Duration.ofMillis(10));
            consumerRecords.forEach(cr -> {
                int countDiff = 0;
                StationLocation currentStationLocation = cr.value();

                if (!stationLocationList.contains(currentStationLocation)) {
                    stationLocationList.add(currentStationLocation);
                } else {
                    StationLocation stored = stationLocationList.get(stationLocationList.indexOf(currentStationLocation));
                    countDiff = stored.available_bikes - currentStationLocation.available_bikes;
                }

                if (countDiff != 0) {
                    String chNbVelos = Math.abs(countDiff) > 1 ? "vélos disponibles" : "vélo disponible";
                    System.out.println("Disponibilité : " + countDiff + " " + chNbVelos + " dans la station n° " + currentStationLocation.number + " à " + currentStationLocation.contract_name +", soit : "+ currentStationLocation.available_bikes + " disponible(s) sur " + currentStationLocation.bike_stands + " au total.");
                }

                System.out.println("CONSUMER :  Records : Key=>" + cr.key() + ", value=>" + cr.value() + ", offset=>" + cr.offset() + " partition=>"+ cr.partition());
            });
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

}
