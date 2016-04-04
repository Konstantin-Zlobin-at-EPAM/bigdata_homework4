package com.epam.bigdata.impressions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class BGMapper extends org.apache.hadoop.mapreduce.Mapper<
        LongWritable, Text,
        IdTimestampComposedKey, LineIsImpressionValue> {
    private final static Logger LOG = LoggerFactory.getLogger(BGMapper.class);
    private final static int TIMESTAMP = 1;
    private final static int IPIN_YOU_ID = 2;
    private final static int STREAM_ID = 21;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        final String[] columnValues = line.split("\\t");
        final String iPinYouId = Optional.ofNullable(columnValues[IPIN_YOU_ID]).orElse("?");
        final String streamId = Optional.ofNullable(columnValues[STREAM_ID]).orElse("?");
        long timestamp;
        try {
            timestamp = Long.parseLong(Optional.ofNullable(columnValues[TIMESTAMP]).orElse("0"));
        } catch (NumberFormatException nfe) {
            timestamp = 0L;
        }
//        final BrowserCounterGroup detectedBrowser = Arrays.asList(BrowserCounterGroup.values()).stream()
//                .filter(c -> userAgent.contains(c.name())).findFirst().orElse(BrowserCounterGroup.Unknown);
//        context.getCounter(detectedBrowser).increment(1L);
        context.write(
                new IdTimestampComposedKey(new Text(iPinYouId), new LongWritable(timestamp)),
                new LineIsImpressionValue(value, "1".equals(streamId.trim()))
        );
    }
}
