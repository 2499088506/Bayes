package com.homework;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Train {
	private Path pathtrain;
	private Path pathtemp;
	private static Configuration conf;
	private Job jobq;
	private static long wordsumcount = 0; //所有训练集中单词的总类别
	private static long filesumcount = 0; //所有训练集中的文件总个数
	private static double tempP;
	
	private static String vecStr = new String();

	private static Vector<String> vecSumwordfile = new Vector<String>();
	
	public Train(Path p1, Path p2, Configuration c)
	{
		pathtrain = p1;
		pathtemp = p2;
		conf = c;
	}
	
	public static long getWordsumcount()
	{
		return wordsumcount;
	}
	public static long getFilesumcount()
	{
		return filesumcount;
	}
	
	
	
	
	public static class Maptrain1 extends Mapper<Object, Text, Text, IntWritable>
	{
		private final IntWritable one = new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			String[] str = value.toString().split("\r\n");
			for(String s : str)
			{
				if(s.matches("[a-zA-Z]+"))
					context.write(new Text(s), one);
			}
		}
	}
	public static class Reducetrain1 extends Reducer<Text, IntWritable, Text, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();
		double td;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
		throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable i : value)
			{
				sum += i.get();
			}
			td = Math.log(sum + 1);
			result.set(td - tempP);
			context.write(key, result);
		}
	}
	//获得每一个类的单词和数量===>>相当于对每一类测试文件Wordcount		
	public void runtrain1()
	throws IOException, InterruptedException, ClassNotFoundException
	{
		FileSystem fileFS = FileSystem.get(pathtrain.toUri(), conf);
		int i=0,indexbeg,indexend,ti;
		for(FileStatus fs : fileFS.listStatus(pathtrain))
		{
			vecStr = vecSumwordfile.get(i);
			++i;
			indexbeg = vecStr.indexOf('+');
			indexend = vecStr.indexOf(':');
			ti = Integer.parseInt(vecStr.substring(indexbeg + 1, indexend));
			tempP = Math.log(wordsumcount + ti);
			 
			Job j = new Job(conf, "trainrun1");
			j.setJarByClass(Bayes.class);
			
			j.setInputFormatClass(WholeInputFormat1.class);
			
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			
			j.setMapperClass(Maptrain1.class);
			j.setReducerClass(Reducetrain1.class);
			
			FileInputFormat.setInputPaths(j, fs.getPath());
			
			String s = new String();
			s = fs.getPath().getName();
			FileOutputFormat.setOutputPath(j, new Path(pathtemp.toString() + '/' + s));
			j.waitForCompletion(true);
			System.out.println(pathtemp.toString() + '/' + s);
			System.out.println("训练集的第一个map/reducer程序运行成功！");
			System.out.println("已经获取了训练集中的每一个分类中单词的Wordcount统计");
			System.out.println("");
		}
	}
	
	
	
	
	public static class Maptrain2 extends Mapper<Object, Text, Text, Text>
	{
		private FileSplit split;
		private String valueInfo = new String();
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			String strDir = new String();
			int wordsum = 0;
			int filesum = 0;
			split = (FileSplit)context.getInputSplit();
			strDir = split.getPath().getName();
			FileSystem fileFS = FileSystem.get(split.getPath().toUri(), conf);
			filesum = fileFS.listStatus(split.getPath()).length;
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while(itr.hasMoreTokens())
			{
				if(itr.nextToken().matches("[a-zA-Z]+"))
					++wordsum;				
			}
			valueInfo = "" + wordsum + ":" + filesum;
//			vecSumwordfile.addElement(strDir + "+" + valueInfo + "/" + filesumcount);
			context.write(new Text(strDir), new Text(valueInfo));
		}
	}
	public static class Reducetrain2 extends Reducer<Text, Text, Text, Text>
	{		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
		throws IOException, InterruptedException
		{
			Integer sum = 0;
			String strword = new String();
			String strfile = new String();
			int index;
			for(Text i : value)
			{
				index = i.toString().indexOf(':');
				strword = i.toString().substring(0, index);
				strfile = i.toString().substring(index+1);
				sum += Integer.parseInt(strword);
			}
			System.out.println(key + "+" + sum.toString() + ":" + strfile + "/" + filesumcount);
			vecSumwordfile.addElement(key + "+" + sum.toString() + ":" + strfile + "/" + filesumcount);
			//context.write(key, new Text(sum.toString() + ":" + strfile + "/" + filesumcount));
		}
	}
	//获得测试集中每一类文件的总单词数量
	public void runtrain2()
	throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job = new Job(conf, "runtrain2");
		
		job.setJarByClass(Bayes.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(WholeInputFormat1.class);
		
		job.setMapperClass(Maptrain2.class);
		job.setReducerClass(Reducetrain2.class);
		
		FileSystem fileFS = FileSystem.get(pathtrain.toUri(), conf);
		for(FileStatus fs : fileFS.listStatus(pathtrain))
		{
			FileInputFormat.addInputPath(job, fs.getPath());
		}
		job.setOutputFormatClass(NullOutputFormat.class);
		job.waitForCompletion(true);
		//wordsumcount = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
		System.out.println("训练集的第二个map/reducer程序运行成功！");
		System.out.println("已经获取了训练集中每一个分类的单词总数");
		System.out.println("vecSumwordfile的总个数: " + vecSumwordfile.size());
		System.out.println("");
	}
	
	
	
	
	public static class Maptrain3 extends Mapper<Object, Text, Text, IntWritable>
	{
		private final IntWritable one = new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			String[] str = value.toString().split("\r\n");
			for(String s : str)
			{
				if(s.matches("[a-zA-Z]+"))
					context.write(new Text(s), one);
			}
		}
	}
	public static class Reducetrain3 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
		throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable i : value)
			{
				sum += i.get();
			}
			wordsumcount += 1;
			result.set(sum);
			//context.write(key, result);
		}
	}
	//获得训练集中所有类别的总单词总数
	public void runtrain3()
	throws IOException, InterruptedException, ClassNotFoundException
	{
		jobq = new Job(conf, "runtrain3");
		jobq.setJarByClass(Bayes.class);
		jobq.setMapperClass(Maptrain3.class);
		jobq.setReducerClass(Reducetrain3.class);
		
		jobq.setInputFormatClass(WholeInputFormat1.class);
		
		jobq.setOutputKeyClass(Text.class);
		jobq.setOutputValueClass(IntWritable.class);
		
		FileSystem fileFS = FileSystem.get(pathtrain.toUri(), conf);
		for(FileStatus fs : fileFS.listStatus(pathtrain))
		{
			FileInputFormat.addInputPath(jobq, fs.getPath());
			FileSystem filefs = FileSystem.get(fs.getPath().toUri(), conf);
			filesumcount += filefs.listStatus(fs.getPath()).length;
		}
		jobq.setOutputFormatClass(NullOutputFormat.class);
		jobq.waitForCompletion(true);
		System.out.println("训练集的第三个map/reducer程序运行成功！");
		System.out.println("已经获取了训练集中所有分类的单词类别数");
		System.out.println("所有训练集中单词类型的总数" + wordsumcount);
		System.out.println("所有训练集文件中的总个数" + filesumcount);
		System.out.println("");
	}
	
	public void setGlobalFile() throws Exception
	{
		//这个函数最大的意义在于实现了训练集和测试集的有效分离
		byte[] buff = null;
		Path pt = new Path(pathtrain.getParent().toString() + "/globalVariable");
		FileSystem fs = pt.getFileSystem(conf);
		if(fs.exists(pt))
		{
			System.out.println("文件已经存在了");
		}
		FSDataOutputStream out = fs.create(pt);
		String stemp = new String();
		stemp = "" + wordsumcount + "\n";
		buff = stemp.getBytes();
		out.write(buff, 0, buff.length);
		stemp = "" + filesumcount + "\n";
		buff = stemp.getBytes();
		out.write(buff, 0, buff.length);
		for(String t : vecSumwordfile)
		{
			t = t + "\n";
			buff = t.getBytes();
			out.write(buff, 0, buff.length);
		}
		out.close();
		fs.close();
	}
	
}
