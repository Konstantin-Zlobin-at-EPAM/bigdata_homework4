package com.epam.bigdata.visitsspends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class VSMapperTest {
    private MapDriver<LongWritable, Text, Text, VSWritable> mapDriver;

    @Before
    public void setUp() {
        VSMapper mapper = new VSMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, VSWritable>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1),
                new Text("69cc5e0c51154da6c37d5e415b74ee4a\t20130607234421256\tVhKiLa5vD4cfXba\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t59.40.102.*\t216\t219\t3\tersbQv1RdoTy1m58uG\t7afed8d69bf0a10e0f0745834843dc90\tnull\tSports_NBA_Width1A\t1000\t90\t0\t0\t70\t0cd33fcb336655841d3e1441b915748d\t254\t3476\t282163092590\t0\n"));
        mapDriver.withOutput(new Text("59.40.102.*"), new VSWritable(1L, 254));
        mapDriver.withCounter(BrowserCounterGroup.Chrome, 1);
        mapDriver.withCounter(BrowserCounterGroup.Android, 0);
        mapDriver.withCounter(BrowserCounterGroup.iPhone, 0);
        mapDriver.withCounter(BrowserCounterGroup.Firefox, 0);
        mapDriver.withCounter(BrowserCounterGroup.MSIE, 0);
        mapDriver.withCounter(BrowserCounterGroup.Safari, 0);
        mapDriver.withCounter(BrowserCounterGroup.Opera, 0);
        mapDriver.withCounter(BrowserCounterGroup.Unknown, 0);
        mapDriver.runTest();
    }
}
