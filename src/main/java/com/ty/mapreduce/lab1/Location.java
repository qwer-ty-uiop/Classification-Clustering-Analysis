package com.ty.mapreduce.lab1;

import com.google.common.primitives.Doubles;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Location implements WritableComparable<Location> {
    double longitude;
    double latitude;

    @Override
    public int compareTo(Location o) {
        return Doubles.compare(this.longitude, o.longitude) == 0 ? Double.compare(this.latitude, o.latitude) : Double.compare(this.longitude, o.longitude);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(longitude);
        dataOutput.writeDouble(latitude);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        longitude = dataInput.readDouble();
        latitude = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "Location{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
}
