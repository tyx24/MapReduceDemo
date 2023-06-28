//手机号流量统计，并排序
package com.hadoop.mapreduce.writableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar包路径
        job.setJarByClass(FlowDriver.class);

        //3.关联mapper和reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出的kvlx
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("A:\\put\\output2"));
        FileOutputFormat.setOutputPath(job, new Path("A:\\put\\output4"));

        //7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
