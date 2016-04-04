package com.epam.bigdata.impressions;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BGReducer extends org.apache.hadoop.mapreduce.Reducer<
        IdTimestampComposedKey, LineIsImpressionValue,
        IdTimestampComposedKey, Text> {
    private final static Logger LOG = LoggerFactory.getLogger(BGReducer.class);

    @Override
    protected void reduce(IdTimestampComposedKey key, Iterable<LineIsImpressionValue> values, Context context)
            throws IOException, InterruptedException {
        long sumImpressions = 0;
        for (LineIsImpressionValue val : values) {
            sumImpressions += val.isImpression() ? 1 : 0;
            context.write(key, val.getInitialLine());
        }
        context.getCounter("IMPRESSIONS", key.getiPinYouId().toString()).increment(sumImpressions);
    }
}
