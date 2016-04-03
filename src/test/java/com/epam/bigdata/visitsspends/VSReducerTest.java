package com.epam.bigdata.visitsspends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class VSReducerTest {

    private ReduceDriver<Text, VSWritable, Text, VSWritable> reduceDriver;

    @Before
    public void setUp() {
        VSReducer reducer = new VSReducer();
        reduceDriver = new ReduceDriver<Text, VSWritable, Text, VSWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        reduceDriver.withInput(new Text("59.40.102.*"),
                Arrays.asList(new VSWritable(1L, 254), new VSWritable(1L, 100)));
        reduceDriver.withOutput(new Text("59.40.102.*"), new VSWritable(2L, 354));
        reduceDriver.runTest();
    }}
