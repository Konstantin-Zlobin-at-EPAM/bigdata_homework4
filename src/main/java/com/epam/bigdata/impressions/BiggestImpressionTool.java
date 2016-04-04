package com.epam.bigdata.impressions;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
        job.setOutputKeyClass(IdTimestampComposedKey.class);
        job.setOutputValueClass(LineIsImpressionValue.class);

        job.setMapperClass(BGMapper.class);
//        job.setCombinerClass(BGReducer.class);
        job.setReducerClass(BGReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + System.currentTimeMillis()));

        int returnCode = job.waitForCompletion(true) ? 0 : 1;
//        LOG.info("Custom counters:");
//        LOG.info("\t" + BrowserCounterGroup.class.getSimpleName());
//        Arrays.asList(BrowserCounterGroup.values()).stream().forEach((BrowserCounterGroup counter) -> {
//            try {
//                LOG.info("\t\t" + counter.name() + "=" + job.getCounters().findCounter(counter).getValue());
//            } catch (IOException e) {
//                LOG.error("cannot get counter: " + counter, e);
//                throw new RuntimeException(e);
//            }
//        });
        return returnCode;
    }

}
