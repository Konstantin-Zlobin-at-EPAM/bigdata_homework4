package com.epam.bigdata.usertag;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class UserTagMapperTest {

        private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

        @Before
        public void setUp() {
            UserTagMapper mapper = new UserTagMapper();
            mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
            mapDriver.setMapper(mapper);
        }

        @Test
        public void testMapper() throws IOException {
            mapDriver.withCacheFile("user.profile.tags.us.txt");
            mapDriver.withInput(new LongWritable(1),
                    new Text("69cc5e0c51154da6c37d5e415b74ee4a\t20130607234421256\tVhKiLa5vD4cfXba\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t59.40.102.*\t216\t219\t3\tersbQv1RdoTy1m58uG\t7afed8d69bf0a10e0f0745834843dc90\tnull\tSports_NBA_Width1A\t1000\t90\t0\t0\t70\t0cd33fcb336655841d3e1441b915748d\t254\t3476\t282163092590\t0\n"));
            mapDriver.withOutput(new Text("282163092590"), new LongWritable(1));
            mapDriver.runTest();
        }
}
