package com.epam.bigdata.impressions;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiggestImpressionTool extends Configured implements Tool {

    final static Logger LOG = LoggerFactory.getLogger(BiggestImpressionTool.class);

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new BiggestImpressionTool(), args);
            System.exit(res);
        } catch (Exception ex) {
            LOG.error("ERROR: ", ex);
            System.exit(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "biggest_impression");

        job.setJarByClass(BiggestImpressionTool.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IdTimestampComposedKey.class);
        job.setMapOutputValueClass(LineIsImpressionValue.class);

        job.setMapperClass(BGMapper.class);
        job.setReducerClass(BGReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(BGPartitioner.class);
        job.setGroupingComparatorClass(BGGroupingComparator.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + System.currentTimeMillis()));

        int returnCode = job.waitForCompletion(true) ? 0 : 1;

        LOG.info("Reducer counters:");
        LOG.info("\t" + BGReducer.BIGGEST_IMPRESSIONS);
        long biggestImpressions = 0;
        String biggestImpressionsIPinId = "none";
        for (Counter counter : job.getCounters().getGroup(BGReducer.BIGGEST_IMPRESSIONS)) {
            if (counter.getValue() > biggestImpressions) {
                biggestImpressions = counter.getValue();
                biggestImpressionsIPinId = counter.getName();
            }
        }
        LOG.info("\tiPinId: " + biggestImpressionsIPinId + " = " + biggestImpressions + " (imprs.)");
        return returnCode;
    }

}
