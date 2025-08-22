package org.example.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class WeatherData implements Serializable {
    private Timestamp formattedDate;           // Formatted Date
    private String summary;                    // Summary
    private String precipType;                 // Precip Type
    private Double temperature;                // Temperature (C)
    private Double apparentTemperature;        // Apparent Temperature (C)
    private Double humidity;                   // Humidity
    private Double windSpeed;                  // Wind Speed (km/h)
    private Double windBearing;                // Wind Bearing (degrees)
    private Double visibility;                 // Visibility (km)
    private Double loudCover;                  // Loud Cover
    private Double pressure;                   // Pressure (millibars)
    private String dailySummary;               // Daily Summary

    // Constructors
    public WeatherData() {}

    public WeatherData(Timestamp formattedDate, String summary, String precipType,
                       Double temperature, Double apparentTemperature, Double humidity,
                       Double windSpeed, Double windBearing, Double visibility,
                       Double loudCover, Double pressure, String dailySummary) {
        this.formattedDate = formattedDate;
        this.summary = summary;
        this.precipType = precipType;
        this.temperature = temperature;
        this.apparentTemperature = apparentTemperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
        this.windBearing = windBearing;
        this.visibility = visibility;
        this.loudCover = loudCover;
        this.pressure = pressure;
        this.dailySummary = dailySummary;
    }

    // Getters and Setters
    public Timestamp getFormattedDate() { return formattedDate; }
    public void setFormattedDate(Timestamp formattedDate) { this.formattedDate = formattedDate; }

    public String getSummary() { return summary; }
    public void setSummary(String summary) { this.summary = summary; }

    public String getPrecipType() { return precipType; }
    public void setPrecipType(String precipType) { this.precipType = precipType; }

    public Double getTemperature() { return temperature; }
    public void setTemperature(Double temperature) { this.temperature = temperature; }

    public Double getApparentTemperature() { return apparentTemperature; }
    public void setApparentTemperature(Double apparentTemperature) { this.apparentTemperature = apparentTemperature; }

    public Double getHumidity() { return humidity; }
    public void setHumidity(Double humidity) { this.humidity = humidity; }

    public Double getWindSpeed() { return windSpeed; }
    public void setWindSpeed(Double windSpeed) { this.windSpeed = windSpeed; }

    public Double getWindBearing() { return windBearing; }
    public void setWindBearing(Double windBearing) { this.windBearing = windBearing; }

    public Double getVisibility() { return visibility; }
    public void setVisibility(Double visibility) { this.visibility = visibility; }

    public Double getLoudCover() { return loudCover; }
    public void setLoudCover(Double loudCover) { this.loudCover = loudCover; }

    public Double getPressure() { return pressure; }
    public void setPressure(Double pressure) { this.pressure = pressure; }

    public String getDailySummary() { return dailySummary; }
    public void setDailySummary(String dailySummary) { this.dailySummary = dailySummary; }

    @Override
    public String toString() {
        return "WeatherData{" +
                "formattedDate=" + formattedDate +
                ", summary='" + summary + '\'' +
                ", precipType='" + precipType + '\'' +
                ", temperature=" + temperature +
                ", apparentTemperature=" + apparentTemperature +
                ", humidity=" + humidity +
                ", windSpeed=" + windSpeed +
                ", windBearing=" + windBearing +
                ", visibility=" + visibility +
                ", loudCover=" + loudCover +
                ", pressure=" + pressure +
                ", dailySummary='" + dailySummary + '\'' +
                '}';
    }
}