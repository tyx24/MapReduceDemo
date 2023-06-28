package com.hadoop.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//WordcountMapper继承Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
/**
 * KEYIN,  map阶段输入的key的类型： LongWritable
 * VALUEIN,  map阶段输入的value的类型： Text
 * KEYOUT,  map阶段输出的key的类型： Text
 * VALUEOUT,  map阶段输出的value的类型： IntWritable
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //转换类型  转换为（Text，IntWritable）；
    private Text outK = new Text();
    //只是统计不聚合 => 1
    private IntWritable outV = new IntWritable(1);

    //重写map
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //1.获取一行
        //hadoop hadoop
        String line = value.toString();

        //2.切割（split(" ")）
        //hadoop
        //hadoop
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words) {

            //封装outk
            outK.set(word);

            //写出
            context.write(outK, outV);
        }
    }
}
