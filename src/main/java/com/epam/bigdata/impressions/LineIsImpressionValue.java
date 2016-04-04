package com.epam.bigdata.impressions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LineIsImpressionValue implements Writable {
    private Text initialLine = new Text("");
    private boolean isImpression;

    public LineIsImpressionValue() {
    }

    public LineIsImpressionValue(Text initialLine, boolean isImpression) {
        this.initialLine = initialLine;
        this.isImpression = isImpression;
    }

    public Text getInitialLine() {
        return initialLine;
    }

    public void setInitialLine(Text initialLine) {
        this.initialLine = initialLine;
    }

    public boolean isImpression() {
        return isImpression;
    }

    public void setImpression(boolean impression) {
        isImpression = impression;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        initialLine.write(out);
        out.writeBoolean(isImpression);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        initialLine.readFields(in);
        isImpression = in.readBoolean();
    }

    public static LineIsImpressionValue read(DataInput in) throws IOException {
        final LineIsImpressionValue result = new LineIsImpressionValue();
        result.readFields(in);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineIsImpressionValue that = (LineIsImpressionValue) o;

        if (isImpression != that.isImpression) return false;
        return initialLine.equals(that.initialLine);

    }

    @Override
    public int hashCode() {
        int result = initialLine.hashCode();
        result = 31 * result + (isImpression ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "initialLine: " + initialLine + ", isImpression: " + isImpression;
    }
}
