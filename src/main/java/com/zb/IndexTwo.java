package com.zb;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexTwo {
    // --------------------map()-------------------------------
// google-->c.txt1
// hadoop-->a.txt2
//以上形式为上一次mapreduce的输出形式 要作为这一次maprede的map的输入形式
// 经过map()操作后 输出形式为 google c.txt-->1
    public static class MapTwo extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            //1.将读到的每一行数据转换为String类型进行操作
            String line = value.toString();
            //2.以指定的格式进行切分
            String[] filds = StringUtils.split(line,"\t");
            //3，上面哪个第一次切分 后的结果为 google-->c.txt    1
            // 下面第二次且分  后的结果为 google c.txt 1
            String[] words = StringUtils.split(filds[0],"-->");
            //4,获取字段的内容
            String word = words[0];
            String filename=words[1];
            long count = Long.parseLong(filds[1]);
            //5.将字段的内容重新组合进行输出 输出格式为  google a.txt-->2;
            context.write(new Text(word), new Text(filename+"-->"+count));
        }
    }
    public static class ReducerTwo extends Reducer<Text, Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
        //1,从map端接受数据 定义链接符
            String link = "";
            for(Text value:values) {
        //2将集合中的数据取出 进行链接
                link+=value+" ";
            }
        //3输出数据 输出格式为 google a.txt-->2 b.txt-->1
            context.write(key,new Text(link));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(IndexTwo.class);
        job.setMapperClass(MapTwo.class);
        job.setReducerClass(ReducerTwo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));

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
