package com.epam.bigdata.impressions;


import org.apache.hadoop.mapreduce.Partitioner;

public class BGPartitioner extends  Partitioner<IdTimestampComposedKey, LineIsImpressionValue> {
    @Override
    public int getPartition(IdTimestampComposedKey idTimestampComposedKey,
                            LineIsImpressionValue lineIsImpressionValue, int numPartitions) {
        return (idTimestampComposedKey.getiPinYouId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
