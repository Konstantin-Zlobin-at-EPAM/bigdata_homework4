package com.epam.bigdata.impressions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IdTimestampComposedKey implements Writable, WritableComparable<IdTimestampComposedKey> {
    private Text iPinYouId = new Text("");
    private LongWritable timestamp = new LongWritable(0L);

    public IdTimestampComposedKey() {
    }

    public IdTimestampComposedKey(Text iPinYouId, LongWritable timestamp) {
        this.iPinYouId = iPinYouId;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(IdTimestampComposedKey idTimestampComposedKey) {
        if (idTimestampComposedKey == null) {
            return 1;
        }
        int compareValue = iPinYouId.compareTo(idTimestampComposedKey.iPinYouId);
        if (compareValue == 0) {
            compareValue = timestamp.compareTo(idTimestampComposedKey.timestamp);
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        iPinYouId.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        iPinYouId.readFields(in);
        timestamp.readFields(in);
    }

    public Text getiPinYouId() {
        return iPinYouId;
    }

    public void setiPinYouId(Text iPinYouId) {
        this.iPinYouId = iPinYouId;
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LongWritable timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdTimestampComposedKey that = (IdTimestampComposedKey) o;

        if (iPinYouId != null ? !iPinYouId.equals(that.iPinYouId) : that.iPinYouId != null) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;

    }

    @Override
    public int hashCode() {
        int result = iPinYouId != null ? iPinYouId.hashCode() : 0;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        // write nothing to output
        return "";
    }
}
