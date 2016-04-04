package com.epam.bigdata.impressions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class BGReducerTest {

    private ReduceDriver<IdTimestampComposedKey, LineIsImpressionValue, NullWritable, Text> reduceDriver;

    @Before
    public void setUp() {
        BGReducer reducer = new BGReducer();
        reduceDriver = new ReduceDriver<IdTimestampComposedKey, LineIsImpressionValue, NullWritable, Text>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        reduceDriver.withInput(new IdTimestampComposedKey(new Text("VhKiLa5vD4cfXba"), new LongWritable(20130607234421256L)),
                Arrays.asList(
                        new LineIsImpressionValue(new Text("LINE OF DATA A"), false),
                        new LineIsImpressionValue(new Text("LINE OF DATA B"), true)));
        reduceDriver.withAllOutput(
                Arrays.asList(
                        new Pair<NullWritable, Text>(NullWritable.get(), new Text("LINE OF DATA A")),
                        new Pair<NullWritable, Text>(NullWritable.get(), new Text("LINE OF DATA B"))
                ));
        reduceDriver.withCounter(BGReducer.BIGGEST_IMPRESSIONS, "VhKiLa5vD4cfXba", 1);
        reduceDriver.runTest();
    }}
