package com.hadoop.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        //1.遍历集合累加值
        long totalup = 0;
        long totaldown = 0;
        for (FlowBean value : values) {
            totalup += value.getUpFlow();
            totaldown += value.getDownFlow();
        }

        //2.封装outK，outV
        outV.setUpFlow(totalup);
        outV.setDownFlow(totaldown);
        outV.setSunFlow();

        //3.写出
        context.write(key, outV);

    }
}
