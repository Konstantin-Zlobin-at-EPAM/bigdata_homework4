package com.epam.bigdata.impressions;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BGGroupingComparator extends WritableComparator {

    public BGGroupingComparator() {
        super(IdTimestampComposedKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IdTimestampComposedKey key1 = (IdTimestampComposedKey) a;
        IdTimestampComposedKey key2 = (IdTimestampComposedKey) b;
        return key1.getiPinYouId().compareTo(key2.getiPinYouId());
    }

}

