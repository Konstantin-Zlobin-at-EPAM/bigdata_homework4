package com.epam.bigdata.usertag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class UserTagCountTool extends Configured implements Tool {

    private final static Logger LOG = LoggerFactory.getLogger(UserTagCountTool.class);

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new UserTagCountTool(), args);
            System.exit(res);
        } catch (Exception ex) {
            LOG.error("ERROR: ", ex);
            System.exit(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "tagcount");

        job.setJarByClass(UserTagCountTool.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(UserTagMapper.class);
        job.setCombinerClass(UserTagReducer.class);
        job.setReducerClass(UserTagReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "_" + System.currentTimeMillis()));

        //it seems like -file parameter doesn't work for some reason, let's do workaround
        job.addCacheFile(new URI(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}