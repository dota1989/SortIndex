package com.zb;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class IndexOne {

    public static class MapOne extends Mapper<LongWritable, Text,Text,LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            // 需求： 将 hello world  --》 hello--a.txt 1  world-->a.txt  1
            //1.取出一行数据 转为字符串
            String line = value.toString();
            //2.将这一行数据按照指定的分割符进行第一次切分
            String[] words = StringUtils.split(line," ");
            //3.获取读取的文件所属的文件切片
            Configuration conf = new Configuration();
            FileSystem fs  = FileSystem.get(conf);
            //4.使用context参数来获取子对象
            FileSplit inputSplit=(FileSplit)context.getInputSplit();
            //5,使用切片对象来获取文件名称
            String filename = inputSplit.getPath().getName();
            //6.讲数据进行传递
            for(String word:words) {
                //7，封装数据输出格式为 k: hello-->a.txt v:1
                context.write(new Text(word+"-->"+filename),new LongWritable(1));
            }
        }
    }
    //======================reduce============================
    public static class ReduceOne extends Reducer<Text, LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Context context) throws IOException, InterruptedException {

            //1.reduce 从map端接受来的数据格式为 hello-->a.txt 1,1,1
            //2,定义for循环将values集合中的数据进行累加
            long count = 0;
            for(LongWritable value:values) {
                count += value.get();
            }
            context.write(key,new LongWritable(count));
        }
    }

    //===================主方法提交job==========================
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //1.获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
            //2.指定jar包的类
        job.setJarByClass(IndexOne.class);
            //3.指定map（）的类
        job.setMapperClass(MapOne.class);
            //4.指定reducer()的类
        job.setReducerClass(ReduceOne.class);
            //5,指定输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
            //6,指定文件输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
            //7,进行健壮型判断如果存储文件已经存在则删除

        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)) {
            fs.delete(output,true);
        }
            //8，指定文件的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
            //9，提交任务
        System.exit(job.waitForCompletion(true)?0:1);
    }

}