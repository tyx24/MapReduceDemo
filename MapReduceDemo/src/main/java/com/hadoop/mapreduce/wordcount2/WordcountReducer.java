package com.hadoop.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//WordcountReducer继承Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
/**
 * KEYIN,  reduce阶段输入的key的类型： Text
 * VALUEIN,  reduce阶段输入的value的类型： IntWritable
 * KEYOUT,  reduce阶段输出的key的类型： Text
 * VALUEOUT,  reduce阶段输出的value的类型： IntWritable
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //转换类型  转换为（IntWritable）；
    private IntWritable outV = new IntWritable();

    //重写reduce
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;

        //hadoop (1,1)
        //累加
        for (IntWritable value : values) {
            sum += value.get();
        }

        //封装outV
        outV.set(sum);

        //写出
        context.write(key, outV);
    }
}
