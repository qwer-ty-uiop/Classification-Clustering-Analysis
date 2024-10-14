package com.ty.mapreduce.lab1;

import com.google.common.primitives.Doubles;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Data implements WritableComparable<Data> {
    String review_id;
    Double longitude;
    Double latitude;
    Double altitude;
    String review_date;
    String temperature;
    Double rating;
    String user_id;
    String user_birthday;
    String user_nationality;
    String user_career;
    Double user_income;

    public Data() {

    }

    public void setAll(String[] splits) {
        setReview_id(splits[0]);
        setLongitude(Double.parseDouble(splits[1]));
        setLatitude(Double.parseDouble(splits[2]));
        setAltitude(Double.parseDouble(splits[3]));
        setReview_date(splits[4]);
        setTemperature(splits[5]);
        setRating(Double.parseDouble(splits[6]));
        setUser_id(splits[7]);
        setUser_birthday(splits[8]);
        setUser_nationality(splits[9]);
        setUser_career(splits[10]);
        setUser_income(Double.parseDouble(splits[11]));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(review_id);
        dataOutput.writeDouble(longitude);
        dataOutput.writeDouble(latitude);
        dataOutput.writeDouble(altitude);
        dataOutput.writeUTF(review_date);
        dataOutput.writeUTF(temperature);
        dataOutput.writeDouble(rating);
        dataOutput.writeUTF(user_id);
        dataOutput.writeUTF(user_birthday);
        dataOutput.writeUTF(user_nationality);
        dataOutput.writeUTF(user_career);
        dataOutput.writeDouble(user_income);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        review_id = dataInput.readUTF();
        longitude = dataInput.readDouble();
        latitude = dataInput.readDouble();
        altitude = dataInput.readDouble();
        review_date = dataInput.readUTF();
        temperature = dataInput.readUTF();
        rating = dataInput.readDouble();
        user_id = dataInput.readUTF();
        user_birthday = dataInput.readUTF();
        user_nationality = dataInput.readUTF();
        user_career = dataInput.readUTF();
        user_income = dataInput.readDouble();
    }

    public double getUser_income() {
        return user_income;
    }

    public void setUser_income(double user_income) {
        this.user_income = user_income;
    }

    public String getUser_career() {
        return user_career;
    }

    public void setUser_career(String user_career) {
        this.user_career = user_career;
    }

    public String getUser_nationality() {
        return user_nationality;
    }

    public void setUser_nationality(String user_nationality) {
        this.user_nationality = user_nationality;
    }

    public String getUser_birthday() {
        return user_birthday;
    }

    public void setUser_birthday(String user_birthday) {
        this.user_birthday = user_birthday;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }

    public String getReview_date() {
        return review_date;
    }

    public void setReview_date(String review_date) {
        this.review_date = review_date;
    }

    public double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getReview_id() {
        return review_id;
    }

    public void setReview_id(String review_id) {
        this.review_id = review_id;
    }

    @Override
    public String toString() {
        return "Data{" +
                "review_id='" + (review_id == null ? "null" : review_id) + '\'' +
                ", longitude=" + (longitude == null ? "null" : longitude) +
                ", latitude=" + (latitude == null ? "null" : latitude) +
                ", altitude=" + (altitude == null ? "null" : altitude) +
                ", review_date='" + (review_date == null ? "null" : review_date) + '\'' +
                ", temperature='" + (temperature == null ? "null" : temperature) + '\'' +
                ", rating=" + (rating == null ? "null" : rating) +
                ", user_id='" + (user_id == null ? "null" : user_id) + '\'' +
                ", user_birthday='" + (user_birthday == null ? "null" : user_birthday) + '\'' +
                ", user_nationality='" + (user_nationality == null ? "null" : user_nationality) + '\'' +
                ", user_career='" + (user_career == null ? "null" : user_career) + '\'' +
                ", user_income=" + (user_income == null ? "null" : user_income) +
                '}';
    }

    @Override
    public int compareTo(Data o) {
        return Doubles.compare(this.longitude, o.longitude) == 0 ? Double.compare(this.latitude, o.latitude) : Double.compare(this.longitude, o.longitude);
    }
}
