package com.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class JoinQuery   {
  public static class joinMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text k=new Text();
        private Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] fields=StringUtils.split(value.toString(),"\t");
		    String id=fields[0];
		    String name=fields[1];
		    k.set(id);
		   FileSplit inputspilt= (FileSplit) context.getInputSplit();
		   String filename=inputspilt.getPath().getName();
		   v.set(name+"-->"+filename);
		   context.write(k, v);
			
			}
        
  }
  public static class joinreducer extends Reducer<Text, Text, Text, Text>{
//     k=001 value= iphone6-->a.txt 100010-->b.txt
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
//		第一次拿出a中的数据
		String leftkey="";
		ArrayList<String> list=new ArrayList<>();
		for(Text value:values){
			if((value.toString().contains("a.txt")))
			leftkey=StringUtils.split(value.toString(),"-->")[0];
			else
			{
				list.add(value.toString());
			}	
		}
		for(String field:list){
			String result="";
			result+=leftkey+"\t"+StringUtils.split(field,"-->")[0];
			context.write(new Text(leftkey), new Text(result));
			
		}
	}
	  
  }
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	  Configuration conf = new Configuration();
		
		Job joinjob = Job.getInstance(conf);
		
		joinjob.setJarByClass(JoinQuery.class);
		
		joinjob.setMapperClass(joinMapper.class);
		joinjob.setReducerClass(joinreducer.class);
		
		joinjob.setOutputKeyClass(Text.class);
		joinjob.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(joinjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(joinjob, new Path(args[1]));
		
		joinjob.waitForCompletion(true);
		
}

}
