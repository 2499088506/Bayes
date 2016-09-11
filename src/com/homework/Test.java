package com.homework;

import java.io.IOException;
import java.io.InputStream;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Test {
	private Path pathtest;
	private Path pathtrain_temp;
	private Configuration conf;
	private final String tpath1 = "bayes_test_temp1";
	private final String tpath2 = "bayes_test_train_temp1";
	private Path pathtest_temp;
	private static String vecStr1 = new String();
	private static String vecStr2 = new String();
	private static int wordcount = 0; 
	private static double pp = 0.0;
	private static double max;
	private static double tmax;
	private static String testStr = new String();
	private static String resultStr = new String();
	private static long wordsumcount = 0; //所有训练集中单词的总类别
	private static long filesumcount = 1; //所有训练集中的文件总个数
	private static Vector<String> vecSumwordfile = new Vector<String>();
	private static String outresult = new String();
	
	public Test(Configuration c, Path p1, Path p2)
	{
		conf = c;
		pathtest = p1;
		pathtrain_temp = p2;
	}
	
	public static class Map1 extends Mapper<Object, Text, Text, IntWritable>
	{
		IntWritable one = new IntWritable(1);
		String temp = new String();
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while(itr.hasMoreTokens())
			{
				temp = itr.nextToken();
				if(temp.matches("[a-zA-Z]+"))
					context.write(new Text(temp), one);
			}
		}
	}
	public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
		throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable val : value)
			{
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public void runtest1()
	throws IOException, InterruptedException, ClassNotFoundException
	{
		String pathT = new String();
		FileSystem fileFS = FileSystem.get(pathtest.toUri(), conf);
		pathtest_temp = new Path(pathtest.getParent().toString() + "/" + tpath1);
		for(FileStatus fs : fileFS.listStatus(pathtest))
		{
			Job job = new Job(conf, "runtest1");
			job.setJarByClass(Bayes.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setInputFormatClass(WholeInputFormat.class);
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapperClass(Map1.class);
			job.setReducerClass(Reduce1.class);
			
			pathT = pathtest_temp.toString() + "/" + fs.getPath().getName();
			FileInputFormat.setInputPaths(job, fs.getPath());
			FileOutputFormat.setOutputPath(job, new Path(pathT));
			job.waitForCompletion(true);
			System.out.println("测试集runtest1执行成功");
		}		
	}
	
	
	public static class Map2 extends Mapper<Object, Text, Text, DoubleWritable>
	{
		DoubleWritable done = new DoubleWritable();
		int index;
		String keyInfo = new String();
		String valueInfo = new String();
		String temp = new String();
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			//处理的文件是Linux上的文件。所以换行符应该是"\n"
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while(itr.hasMoreTokens())
			{
				temp = itr.nextToken();
				index = temp.toString().indexOf('\t');
				keyInfo = temp.toString().substring(0, index);
				valueInfo = temp.toString().substring(index + 1);
				done = new DoubleWritable(Double.parseDouble(valueInfo));
				context.write(new Text(keyInfo), done);
			}			
		}
	}
	public static class Reduce2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
		throws IOException, InterruptedException
		{
			int sum = 0;
			double p = 0.1;
			double result;
//			System.out.print(key + ": ");
//			for(DoubleWritable val : value)
//			{
//				System.out.print(val + "   ");
//			}
//			System.out.println("");
			for(DoubleWritable val : value)
			{
				if(val.get() > 0)
					sum = (int)val.get();
				else
					p = val.get();
			}
			if(sum>0)
			{
				if(p < 0)
				{
					result = sum * p;
				}
				else
				{
					result =sum * pp;
				}
				context.write(key, new DoubleWritable(result));
			}
		}
	}
	public void runtest2()
	throws IOException, InterruptedException, ClassNotFoundException
	{
		String pathT = new String();
		int ii = 0,index1,index2;
		FileSystem fileFS = FileSystem.get(pathtest_temp.toUri(), conf);
		FileSystem fileFSt = FileSystem.get(pathtrain_temp.toUri(), conf);
		for(FileStatus fst : fileFSt.listStatus(pathtrain_temp))
		{
			vecStr1 = vecSumwordfile.elementAt(ii);
			++ii;
			index1 = vecStr1.indexOf('+');
			index2 = vecStr1.indexOf(':');
			wordcount = Integer.valueOf(vecStr1.substring(index1 + 1, index2)).intValue();
			pp = 0 - Math.log(wordcount + wordsumcount);
			System.out.println(pp);
			for(FileStatus fs : fileFS.listStatus(pathtest_temp))
			{
				Job job = new Job(conf, "runtest2");
				job.setJarByClass(Bayes.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				job.setInputFormatClass(WholeInputFormat1.class);
				job.setMapperClass(Map2.class);
				job.setReducerClass(Reduce2.class);
				
				pathT = fs.getPath().getParent().getParent().toString() + "/" + tpath2 + 
						"/" + fs.getPath().getName() + "/" + fst.getPath().getName();
				FileInputFormat.addInputPath(job, fs.getPath());
				FileInputFormat.addInputPath(job, fst.getPath());
				FileOutputFormat.setOutputPath(job, new Path(pathT));
				job.waitForCompletion(true);
				System.out.println("测试集runtest2正在执行！");
				System.out.println("正在计算:" + fst.getPath().getName() + "<<==>>" + fs.getPath().getName());
			}
		}
		System.out.println("测试集runtest2执行成功");
	}
	
	
	public static class Map3 extends Mapper<Object, Text, IntWritable, DoubleWritable>
	{
		IntWritable one = new IntWritable(1);
		String str = new String();
		String temp = new String();
		int ind;
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while(itr.hasMoreTokens())
			{
				temp = itr.nextToken();
				ind = temp.toString().indexOf('\t');
				if(ind > 0)
				{
					str = temp.toString().substring(ind + 1);
					context.write(one, new DoubleWritable(Double.valueOf(str)));
				}
			}
		}
	}
	public static class Reduce3 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>
	{
		@Override
		protected void reduce(IntWritable key, Iterable<DoubleWritable> value, Context context)
		throws IOException, InterruptedException
		{
			System.out.println("REDUCE3函数执行");
			for(DoubleWritable val : value)
			{
				tmax += val.get();
			}
		}
	}
	public void runtest3()
	throws IOException, InterruptedException, ClassNotFoundException
	{
		Path pathTT = new Path(pathtest.getParent().toString() + "/" + tpath2);
		int jj,index1,index2;
		int filecnt;
		FileSystem fileFS = FileSystem.get(pathTT.toUri(), conf);
		outresult = "";
		for(FileStatus fs : fileFS.listStatus(pathTT))
		{
			testStr = fs.getPath().getName();
			max = Double.MAX_VALUE;
			testStr = fs.getPath().getName();
			Path path_t = fs.getPath();
			FileSystem fileFST = FileSystem.get(path_t.toUri(), conf);
			jj = 0;
			for(FileStatus fst : fileFST.listStatus(path_t))
			{
				vecStr2 = vecSumwordfile.elementAt(jj);
				index1 = vecStr2.indexOf(':');
				index2 = vecStr2.indexOf('/');
				filecnt = Integer.parseInt(vecStr2.substring(index1 + 1, index2));
				if(jj == 0)
					filesumcount = Integer.parseInt(vecStr2.substring(index2 + 1));
				tmax = 0;
				Job job = new Job(conf, "runtest3");
				job.setJarByClass(Bayes.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setInputFormatClass(WholeInputFormat1.class);
				job.setOutputFormatClass(NullOutputFormat.class);
				job.setMapperClass(Map3.class);
				job.setReducerClass(Reduce3.class);
				
				FileInputFormat.addInputPath(job, fst.getPath());
				job.waitForCompletion(true);
				tmax += Math.log(filecnt) - Math.log(filesumcount);
				tmax = -tmax;
				if(tmax < max)
				{
					max = tmax;
					resultStr = fst.getPath().getName();
				}
				++jj;
				System.out.println("正在执行runtest3: " + fs.getPath().getName() + 
						":" + fst.getPath().getName());
			}
			System.out.println("\n" + "测试集" + testStr + "中最大值tmax:" + "-" + tmax);
			System.out.println("\n" + "测试集" + testStr + "属于分类:" + resultStr);
			outresult += "测试集" + testStr + "中最大值tmax:" + "-" + tmax + "\n";
			outresult += "测试集" + testStr + "属于分类:" + resultStr + "\n\n";
		}
		System.out.println("\nruntest3执行完毕");
	}
	
	public void outputResult() throws Exception
	{
		//把结果输出到文件中
		Path pt = new Path(pathtest.getParent().toString() + "/Result");
		byte[] buff = null;
		FileSystem fs = pt.getFileSystem(conf);
		if(fs.exists(pt))
		{
			System.out.println("输出文件已经存在，请删除后在测试");
		}
		FSDataOutputStream out = fs.create(pt);
		buff = outresult.getBytes();
		out.write(buff, 0, buff.length);
		out.close();
		fs.close();
	}
	
	public void getGlobalFile() throws Exception
	{
		//最大的教训是byte[]类型不能直接用toString()转化。。一定要在够着的时候才能用。。
		//这个函数最大的意义在于实现了测试集和训练集的有效分离
		Path pt = new Path(pathtest.getParent().toString() + "/globalVariable");
		FileSystem fs = pt.getFileSystem(conf);
		if(!fs.exists(pt))
		{
			System.out.println("文件不存在，请先执行训练集");
		}
		InputStream in = fs.open(pt);
		FileStatus fst = fs.getFileStatus(pt);
		System.out.println("文件长度:" + fst.getLen());
		String stemp = new String();
		int flag = 0;
		int len = (int) fst.getLen();
		byte[] buff = new byte[len];
		if(in.read(buff, 0 , len) <= 0)
		{
			System.out.println("读取文件的时候出现错误");
		}
		String s = new String(buff);
		System.out.println("读取出来的文件:" + s);
		StringTokenizer itr = new StringTokenizer(s, "\n");
		while(itr.hasMoreTokens())
		{
			stemp = itr.nextToken();
			System.out.println("从文件内读取出来的内容: " + stemp);
			if(flag >= 2)
			{				
				vecSumwordfile.add(stemp);
			}
			else if(flag == 0)
			{
				wordsumcount = Integer.parseInt(stemp);
			}
			else if(flag == 1)
			{
				filesumcount = Integer.parseInt(stemp);
			}
			++flag;
		}
		
		in.close();
		fs.close();
	}
}
