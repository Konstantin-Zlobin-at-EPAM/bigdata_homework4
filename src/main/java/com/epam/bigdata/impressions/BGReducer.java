package com.epam.bigdata.impressions;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class BGReducer extends org.apache.hadoop.mapreduce.Reducer<
        IdTimestampComposedKey, LineIsImpressionValue,
        IdTimestampComposedKey, Text> {
    public static final String BIGGEST_IMPRESSIONS = "BIGGEST_IMPRESSIONS";
    private final static Logger LOG = LoggerFactory.getLogger(BGReducer.class);

    private long biggestSumImpressions = 0L;
    private String biggestIPinYouId;

    @Override
    protected void reduce(IdTimestampComposedKey key, Iterable<LineIsImpressionValue> values, Context context)
            throws IOException, InterruptedException {
        final String iPinYouID = key.getiPinYouId().toString();
        long localSum = 0;
        for (LineIsImpressionValue val : values) {
            localSum += val.isImpression() ? 1 : 0;
            context.write(null, val.getInitialLine());
        }
        synchronized (this) {
            if ((!"null".equals(iPinYouID)) && localSum > biggestSumImpressions) {
                biggestSumImpressions = localSum;
                biggestIPinYouId = iPinYouID;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        synchronized (this) {
            if (biggestIPinYouId != null) {
                context.getCounter(BIGGEST_IMPRESSIONS, biggestIPinYouId).increment(biggestSumImpressions);
            }
        }
    }
}
