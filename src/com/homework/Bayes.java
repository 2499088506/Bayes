package com.homework;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class Bayes {
	/* Master2
	hdfs://192.168.248.140:9000/user/hadoop2/bayes_train
	hdfs://192.168.248.140:9000/user/hadoop2/bayes_test
	hdfs://192.168.248.140:9000/user/hadoop2/bayes_train_temp1
	 */
	public static void main(String[] args)
	throws Exception,IOException
	{
		long startTime, midTime, endTime, ending;
		long tTime;
		String ttemp = new String();
		byte[] buff = null;
		
		Configuration conf = new Configuration();
		//conf.set("mapred.jar", "D://Java//Master2//Bayes.jar");	//在eclipse下用ant打包使用
		//conf.set("mapred.job.tracker", "192.168.248.140:9001");

		String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherargs.length != 3)
		{
			System.out.println("the count of args is not 3");
			System.exit(3);
		}
		Path pathtrain = new Path(otherargs[0]);
		Path pathtest = new Path(otherargs[1]);
		Path pathtemp = new Path(otherargs[2]);
		
		startTime = System.currentTimeMillis();
		
		training(pathtrain, pathtemp, conf);
		
		midTime = System.currentTimeMillis();		
		
		testing(conf, pathtest, pathtemp);
		
		endTime = System.currentTimeMillis();  
		
		testing1(pathtemp, pathtest, conf);
		
		ending = System.currentTimeMillis();
		
		Path pp = new Path(pathtemp.getParent().toString() + "/Print");
		FileSystem fs = pathtemp.getFileSystem(conf);
		FSDataOutputStream out = fs.create(pp);
		
		tTime = (midTime - startTime)/1000;
		ttemp = "训练集运行的时间是:" + tTime/60 + "min" + tTime%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		tTime = (endTime - midTime)/1000; 
		ttemp = "测试集运行的时间是:" + tTime/60 + "min" + tTime%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		tTime = (ending - endTime)/1000; 
		ttemp = "单机计算准确率运行时间是:" + tTime/60 + "min" + tTime%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		tTime = (endTime - startTime)/1000; 
		ttemp = "程序总共运行时间是:" + tTime/60 + "min" + tTime%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		out.close();
		fs.close();
	}
	public static void training(Path pathtrain, Path pathtemp, Configuration conf)
			throws Exception
	{
		long t1,t2;
		String ttemp = new String();
		byte[] buff = null;
		
		Train train = new Train(pathtrain, pathtemp, conf);
		Path pp = new Path(pathtemp.getParent().toString() + "/PrintTrain");
		FileSystem fs = pathtemp.getFileSystem(conf);
		FSDataOutputStream out = fs.create(pp);
		t2 = System.currentTimeMillis();
		train.runtrain3();
		t1 = System.currentTimeMillis() - t2;
		ttemp = "runtrain3运行时间:" + t1/60000 + "min" + (t1/1000)%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		t1 = System.currentTimeMillis();
		train.runtrain2();
		t2 = System.currentTimeMillis() - t1;
		ttemp = "runtrain2运行时间:" + t2/60000 + "min" + (t2/1000)%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		t2 = System.currentTimeMillis();
		train.runtrain1();
		t1 = System.currentTimeMillis() - t2;
		ttemp = "runtrain1运行时间:" + t1/60000 + "min" + (t1/1000)%60 + "s";
		buff = (ttemp + "\n\n").getBytes();
		out.write(buff, 0, buff.length);
		System.out.println(ttemp);
		
		out.close();
		fs.close();
		
		train.setGlobalFile();
	}
	
	public static void testing(Configuration conf, Path pathtest, Path pathtemp)
			throws Exception
	{
		long startTime = System.currentTimeMillis();
		long t1,t2;
		String ttemp = new String();
		byte[] buff = null;
		
		Test test = new Test(conf, pathtest, pathtemp);
		
		
		test.getGlobalFile();
		
		test.runtest1();
		t1 = System.currentTimeMillis() - startTime;
		ttemp = "runtest1运行时间:" + t1/60000 + "min" + (t1/1000)%60 + "s\n\n";
		System.out.println(ttemp);
		
		t1 = System.currentTimeMillis();
		test.runtest2();
		t2 = System.currentTimeMillis() - t1;
		ttemp += "runtest2运行时间:" + t2/60000 + "min" + (t2/1000)%60 + "s\n\n";
		System.out.println(ttemp);
		
		t2 = System.currentTimeMillis();
		test.runtest3();
		t1 = System.currentTimeMillis() - t2;
		ttemp += "runtest3运行时间:" + t1/60000 + "min" + (t1/1000)%60 + "s\n\n";
		System.out.println(ttemp);
		
		Path pp = new Path(pathtemp.getParent().toString() + "/PrintTest");
		FileSystem fs = pathtemp.getFileSystem(conf);
		FSDataOutputStream out = fs.create(pp);
		buff = ttemp.getBytes();
		out.write(buff, 0, buff.length);
		out.close();
		fs.close();
		
		test.outputResult();
	}
	
	public static void testing1(Path pathtemp, Path pathtest, Configuration conf)
			throws Exception
	{
		Test1 testp = new Test1(pathtemp, pathtest, conf);
		testp.init();
		testp.testing();
	}
}
