package com.tmaskibail.beam.demo.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

public class SensorData implements Serializable {
    private String deviceId;
    private String deviceNumId;
    private String deviceRegistryId;
    private String deviceRegistryLocation;
    private String projectId;
    private String subFolder;
    private String ts;
    private Double temperature;
    private Double pressure;
    private Double humidity;
    private Double ambientLight;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorData that = (SensorData) o;
        return Objects.equals(deviceId, that.deviceId) && Objects.equals(deviceRegistryId, that.deviceRegistryId) && Objects.equals(projectId, that.projectId) && Objects.equals(subFolder, that.subFolder) && ts.equals(that.ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, deviceRegistryId, projectId, subFolder, ts);
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Double getPressure() {
        return pressure;
    }

    public void setPressure(Double pressure) {
        this.pressure = pressure;
    }

    public Double getHumidity() {
        return humidity;
    }

    public void setHumidity(Double humidity) {
        this.humidity = humidity;
    }

    public Double getAmbientLight() {
        return ambientLight;
    }

    public void setAmbientLight(Double ambientLight) {
        this.ambientLight = ambientLight;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceNumId() {
        return deviceNumId;
    }

    public void setDeviceNumId(String deviceNumId) {
        this.deviceNumId = deviceNumId;
    }

    public String getDeviceRegistryId() {
        return deviceRegistryId;
    }

    public void setDeviceRegistryId(String deviceRegistryId) {
        this.deviceRegistryId = deviceRegistryId;
    }

    public String getDeviceRegistryLocation() {
        return deviceRegistryLocation;
    }

    public void setDeviceRegistryLocation(String deviceRegistryLocation) {
        this.deviceRegistryLocation = deviceRegistryLocation;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getSubFolder() {
        return subFolder;
    }

    public void setSubFolder(String subFolder) {
        this.subFolder = subFolder;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "deviceId='" + deviceId + '\'' +
                ", deviceNumId='" + deviceNumId + '\'' +
                ", deviceRegistryId='" + deviceRegistryId + '\'' +
                ", deviceRegistryLocation='" + deviceRegistryLocation + '\'' +
                ", projectId='" + projectId + '\'' +
                ", subFolder='" + subFolder + '\'' +
                ", ts='" + ts + '\'' +
                ", temperature=" + temperature +
                ", pressure=" + pressure +
                ", humidity=" + humidity +
                ", ambientLight=" + ambientLight +
                '}';
    }

    public Map<String, String> getAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("DeviceId", trimToEmpty(getDeviceId()));
        attributes.put("DeviceNumId", trimToEmpty(getDeviceNumId()));
        attributes.put("DeviceRegistryId", trimToEmpty(getDeviceRegistryId()));
        attributes.put("DeviceRegistryLocation", trimToEmpty(getDeviceRegistryLocation()));
        attributes.put("ProjectId", trimToEmpty(getProjectId()));
        attributes.put("SubFolder", trimToEmpty(getSubFolder()));
        return attributes;
    }
}
