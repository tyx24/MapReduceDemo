package com.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileNme;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        //初始化   order  pd
        FileSplit split = (FileSplit) context.getInputSplit();

        fileNme = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();

        //2.判断是那个文件
        if (fileNme.contains("order")) {     //处理的是订单表

            //切割
            String[] split = line.split("\t");

            //封装
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {     //处理的是商品表

            //切割
            String[] split = line.split("\t");

            //封装
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        //写出
        context.write(outK, outV);

    }
}
