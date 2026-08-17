package com.bksoft.kafka_stream.model;

public class OrderAverage {
    public double sum;
    public long count;

    public OrderAverage() {
    }

    public OrderAverage(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getAverage() {
        if (count == 0) {
            return 0.0;
        }
        return sum / count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "OrderAverage{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }
}
