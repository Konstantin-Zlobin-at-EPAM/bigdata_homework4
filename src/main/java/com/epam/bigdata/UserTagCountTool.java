package com.epam.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class UserTagCountTool extends Configured implements Tool {

    final static Logger LOG = LoggerFactory.getLogger(UserTagCountTool.class);

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
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + System.currentTimeMillis()));

        job.addCacheFile(new URI("/apps/homework3/user.profile.tags.us.txt"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper {
        final static Logger LOG = LoggerFactory.getLogger(Mapper.class);

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        final Set<String> userTags = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (userTags.isEmpty()) {
                if (context.getCacheFiles() == null) {
                    throw new RuntimeException("Where are my cached files?");
                } else {
                    final URI userProfilesUri = context.getCacheFiles()[0];
                    final Path userProfilesPath = new Path(userProfilesUri);
                    final FileSystem fs = FileSystem.get(context.getConfiguration());
                    final BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(userProfilesPath)));
                    String line;
                    line = br.readLine();
                    boolean headerLine = true;
                    while (line != null) {
                        final String[] values = line.split("\\t", -1);
                        final boolean isHeader = headerLine;
                        if (!isHeader) {
                            final String userId = values[0];
                            userTags.add(userId);
                        }
                        line = br.readLine();
                        headerLine = false;
                    }
                    LOG.debug("The following user tags have been read: " + String.join(",", userTags.toArray(new CharSequence[] {})));
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();
            final String[] coulmnValues = line.split("\\t");
            final String userId = coulmnValues[20];
            if (userTags.contains(userId)) {
                word.set(userId);
                context.write(word, one);
            }
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer {
        final static Logger LOG = LoggerFactory.getLogger(Reducer.class);

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}