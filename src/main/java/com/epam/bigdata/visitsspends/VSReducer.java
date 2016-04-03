package com.epam.bigdata.visitsspends;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class VSReducer extends org.apache.hadoop.mapreduce.Reducer<
        Text, VSWritable,
        Text, VSWritable> {
    private final static Logger LOG = LoggerFactory.getLogger(VSReducer.class);

    @Override
    protected void reduce(Text key, Iterable<VSWritable> values, Context context)
            throws IOException, InterruptedException {
        long sumVisits = 0;
        long sumSpends = 0;
        for (VSWritable val : values) {
            sumVisits += val.getVisits();
            sumSpends += val.getSpends();
        }
        context.write(key, new VSWritable(sumVisits, sumSpends));
    }
}
