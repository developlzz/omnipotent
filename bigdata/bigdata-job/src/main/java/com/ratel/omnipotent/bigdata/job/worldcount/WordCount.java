package com.ratel.omnipotent.bigdata.job.worldcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override  
        protected void map(LongWritable key, Text value,  
                Mapper<LongWritable, Text, Text, LongWritable>.Context context)  
                throws IOException, InterruptedException { 
        	//一行
            String[] words = value.toString().split(" ");  
            for (String word : words) {  
            	Text k2 = new Text();   
                LongWritable v2 = new LongWritable();  
                k2.set(word);  
                v2.set(1L);             
                context.write(k2, v2);//写出  
            }  
        }  
	}
	
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{  
        
        @Override  
        protected void reduce(Text k2, Iterable<LongWritable> v2s,  
                Reducer<Text, LongWritable, Text, LongWritable>.Context context)  
                throws IOException, InterruptedException {  
            long count = 0L;  
            LongWritable v3 = new LongWritable();  
            for (LongWritable v2 : v2s) {  
                count += v2.get();  
            }  
            v3.set(count);  
            context.write(k2, v3);  
        }  
    }  
	
	 /** 
     * 上面我们把map，reduce都写完了，下面我们把它们合在一起，运转起来 
     */  
    public static void main(String[] args) throws Exception {  
    	System.setProperty("HADOOP_USER_NAME", "root");
    	System.setProperty("HADOOP_HOME", "D:\\hadoop\\hadoop-3.0.2");
    	System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-3.0.2");
        //加载驱动  
        Configuration conf = new Configuration();  
        //获取job,告诉他需要加载那个类  
        Job job = Job.getInstance(conf, WordCount.class.getSimpleName());  

        //获取文件数据  
        FileInputFormat.setInputPaths(job, new Path("hdfs://10.10.21.71:9000/test/words.txt"));  
        //通过TextInputFormat把读到的数据处理成<k1,v1>形式  
        job.setInputFormatClass(TextInputFormat.class);  
        //job中加入Mapper，同时MyMapper类接受<k1,v1>作为参数传给类中map函数进行数据处理  
        job.setMapperClass(WordCountMapper.class);  
        //设置输出的<k2,v2>的数据类型  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(LongWritable.class);  
        //job中加入Reducer,Reducer自动接收处理好的map数据  
        job.setReducerClass(WordCountReducer.class);  
        //设置输出的<k3,v3>的数据类型  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(LongWritable.class);  
        //设置输出目录文件out1  
        String OUT_DIR = "hdfs://10.10.21.71:9000/test/stat";  
        FileOutputFormat.setOutputPath(job, new Path(OUT_DIR));  
        job.setOutputFormatClass(TextOutputFormat.class);  
        //如果这个文件存在则删除，如果文件存在不删除会报错。  
//        deleteOutDir(conf, OUT_DIR);  
        //把处理好的<k3,v3>的数据写入文件  
        job.waitForCompletion(true);  
    }  
}
