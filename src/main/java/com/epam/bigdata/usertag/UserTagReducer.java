package com.epam.bigdata.usertag;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UserTagReducer extends org.apache.hadoop.mapreduce.Reducer<
        Text, LongWritable,
        Text, LongWritable> {
    private final static Logger LOG = LoggerFactory.getLogger(UserTagReducer.class);

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        context.write(key, new LongWritable(sum));
    }
}

