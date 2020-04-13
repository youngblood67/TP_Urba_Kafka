package org.cnam.kafka.consumer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;


public class StationLocationDeserializer implements Deserializer {

    @Override
    public void close() {

    }

    @Override
    public StationLocation deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        StationLocation stationLocation = null;
        try {
            stationLocation = mapper.readValue(arg1, StationLocation.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return stationLocation;
    }
}