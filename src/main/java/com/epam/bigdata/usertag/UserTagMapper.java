package com.epam.bigdata.usertag;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class UserTagMapper extends org.apache.hadoop.mapreduce.Mapper<
        LongWritable, Text,
        Text, LongWritable> {
    private final static Logger LOG = LoggerFactory.getLogger(UserTagMapper.class);
    private final static int USER_TAG = 20;
    private final static LongWritable one = new LongWritable(1);
    final private Set<String> userTags = new HashSet<>();

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
                LOG.debug("The following user tags have been read: " + String.join(",", userTags.toArray(new CharSequence[]{})));
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        final String[] coulmnValues = line.split("\\t");
        final String userTag = coulmnValues[USER_TAG];
        if (userTags.contains(userTag)) {
            context.write(new Text(userTag), one);
        }
    }
}
