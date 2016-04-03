package com.epam.bigdata.visitsspends;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VSWritable implements Writable {
    private long visits;
    private long spends;

    public VSWritable() {
    }

    public VSWritable(long visits, long spends) {
        this.visits = visits;
        this.spends = spends;
    }

    public long getVisits() {
        return visits;
    }

    public void setVisits(long visits) {
        this.visits = visits;
    }

    public long getSpends() {
        return spends;
    }

    public void setSpends(long spends) {
        this.spends = spends;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(visits);
        out.writeLong(spends);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        visits = in.readLong();
        spends = in.readLong();
    }

    public static VSWritable read(DataInput in) throws IOException {
        final VSWritable result = new VSWritable();
        result.readFields(in);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VSWritable that = (VSWritable) o;

        if (visits != that.visits) return false;
        return spends == that.spends;

    }

    @Override
    public int hashCode() {
        int result = (int) (visits ^ (visits >>> 32));
        result = 31 * result + (int) (spends ^ (spends >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "visits: " + visits + ", " + "spends: " + spends;
    }
}
