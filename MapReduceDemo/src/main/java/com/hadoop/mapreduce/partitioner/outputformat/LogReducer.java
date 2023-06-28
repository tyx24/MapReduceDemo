package com.hadoop.mapreduce.partitioner.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        //http://www.baidu.com
        //http://www.google.com
        //http://www.baidu.com

        for (NullWritable value : values) {         //循环每一条数据，防止出现相同数据，丢数据
            context.write(key, NullWritable.get());

        }
    }
}
