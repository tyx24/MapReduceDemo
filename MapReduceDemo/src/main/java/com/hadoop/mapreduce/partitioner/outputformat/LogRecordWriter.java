package com.hadoop.mapreduce.partitioner.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream hadoopOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {

        // 创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            hadoopOut = fs.create(new Path("A:\\put\\output6\\hadooop.log"));

            otherOut = fs.create(new Path("A:\\put\\output6\\other.log"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        String log = text.toString();

        //具体写
        if (log.contains("hadoop")) {         //是否包含hadoop
            hadoopOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        //关流
        IOUtils.closeStream(hadoopOut);
        IOUtils.closeStream(otherOut);

    }
}
