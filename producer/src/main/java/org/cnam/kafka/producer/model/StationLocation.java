package org.cnam.kafka.producer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StationLocation {
    public int number;
    public String contract_name;
    public String name;
    public int bike_stands;
    public int available_bike_stands;
    public int available_bikes;
    public String status;

    public StationLocation() {
    }

    @Override
    public String toString() {
        return "PointLocation{" +
                "number=" + number +
                ", contract_name='" + contract_name + '\'' +
                ", name='" + name + '\'' +
                ", bike_stands=" + bike_stands +
                ", available_bike_stands=" + available_bike_stands +
                ", available_bikes=" + available_bikes +
                ", status='" + status + '\'' +
                '}';
    }
}


