package com.epam.bigdata.usertag;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class UserTagReducerTest {

    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        UserTagReducer reducer = new UserTagReducer();
        reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        reduceDriver.withInput(new Text("282163092590"), Arrays.asList(new LongWritable(1), new LongWritable(1)));
        reduceDriver.withOutput(new Text("282163092590"), new LongWritable(2));
        reduceDriver.runTest();
    }
}
