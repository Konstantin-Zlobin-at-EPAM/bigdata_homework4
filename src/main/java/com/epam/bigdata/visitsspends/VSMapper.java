package com.epam.bigdata.visitsspends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class VSMapper extends org.apache.hadoop.mapreduce.Mapper<
        LongWritable, Text,
        Text, VSWritable> {
    private final static Logger LOG = LoggerFactory.getLogger(VSMapper.class);
    private final static int USER_AGENT = 3;
    private final static int IP = 4;
    private final static int BIDDING_PRICE = 18;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        final String[] columnValues = line.split("\\t");
        final String userAgent = Optional.ofNullable(columnValues[USER_AGENT]).orElse("?");
        final String ip = Optional.ofNullable(columnValues[IP]).orElse("?");
        Long biddingPrice;
        try {
            biddingPrice = Long.parseLong(Optional.ofNullable(columnValues[BIDDING_PRICE]).orElse("0"));
        } catch (NumberFormatException nfe) {
            biddingPrice = 0L;
        }
        final BrowserCounterGroup detectedBrowser = Arrays.asList(BrowserCounterGroup.values()).stream()
                .filter(c -> userAgent.contains(c.name())).findFirst().orElse(BrowserCounterGroup.Unknown);
        context.getCounter(detectedBrowser).increment(1L);
        context.write(new Text(ip), new VSWritable(1L, biddingPrice));
    }
}
