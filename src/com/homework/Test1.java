package com.homework;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test1 {
	private Path bayesTrainPath;
	private Path bayesTestPath;
	private Configuration conf;
	FileSystem fileFS;
	List<Map<String , Double>> mtrain = new ArrayList<Map<String , Double>>();
	List<String> filename = new ArrayList<String>();
	
	Map<String , Integer> re1 = new HashMap<String , Integer>(); //��¼�ļ����Ͳ���ܹ��ļ���
	Map<String , Integer> re2 = new HashMap<String , Integer>(); //��¼�ļ�������ʵ���ܹ��ļ���
	Map<String , Integer> re3 = new HashMap<String , Integer>(); //��¼�ļ����Ͳ����ʵ�ܹ��ļ���
	Map<String , Double> mresult = new HashMap<String , Double>();
	
	Integer ci = new Integer(0);
	public Test1(Path p1, Path p2, Configuration c)
	{
		bayesTrainPath = p1;
		bayesTestPath = p2;
		conf = c;
	}
	public void init() throws IOException
	{
		fileFS = FileSystem.get(bayesTrainPath.toUri(), conf);
		for(FileStatus fs : fileFS.listStatus(bayesTrainPath))
		{
			Map<String , Double> mt = new HashMap<String , Double>();
			String fname = fs.getPath().getName().toString();
			filename.add(fname);
			for(FileStatus fs1 : fileFS.listStatus(fs.getPath()))
			{
				FileSystem ff = fs1.getPath().getFileSystem(conf);
				InputStream in = ff.open(fs1.getPath());
				long fileLen = fs1.getLen();
				if(fileLen < 10)
					continue;
				byte[] buff = new byte[(int) (fileLen)];
				int byteLen = in.read(buff);
				in.close();
				String str = new String(buff, 0, byteLen);
				String ss[] = str.split("\n");
				double lp;
				for(String s : ss)
				{
					String[] stt = s.split("\t");
					lp = Double.parseDouble(stt[1]);
					mt.put(stt[0], lp);
				}
				System.out.println(fs1.getPath());
			}
			System.out.println(fs.getPath().getName().toString() + ": " + mt.size());
			mtrain.add(mt);
		}
		Path p = new Path(bayesTrainPath.getParent().toString() + "/globalVariable");
		fileFS = p.getFileSystem(conf);
		FileStatus fst = fileFS.getFileStatus(p);
		InputStream in = fileFS.open(p);
		int len = (int)fst.getLen();
		byte[] buff = new byte[len];
		in.read(buff, 0, len);
		String str = new String(buff, 0, len);
		String ss[] = str.split("\n");
		for(String s : ss)
		{
			System.out.println(s + "   " + ss[0]);
			ci = Integer.parseInt(ss[0]);
		}	
		
	}
	public void testing() throws IOException
	{
		fileFS = FileSystem.get(bayesTestPath.toUri(), conf);
		for(FileStatus fs : fileFS.listStatus(bayesTestPath))
		{
			String filen = fs.getPath().getName().toString();
			Integer ii = fileFS.listStatus(fs.getPath()).length;
			Integer ti = Integer.valueOf(0);
			re1.put(filen, ti);
			re2.put(filen, ii);
			re3.put(filen, ti);
		}
		for(FileStatus fs : fileFS.listStatus(bayesTestPath))
		{
			String filen = fs.getPath().getName().toString();
			String filet = new String();
			int cnt;
			double minre, td, t1;
			for(FileStatus fs1 : fileFS.listStatus(fs.getPath()))
			{
				InputStream in = fileFS.open(fs1.getPath());
				byte[] buff = new byte[(int) (fs1.getLen())];
				int byteLen = in.read(buff);
				in.close();
				String str = new String(buff, 0, byteLen);
				String ss[] = str.split("\r\n"); //�˴�һ��Ҫ��"\r\n"��Ϊ�ָ���������Ϊ�Ǵ�Windows�¶�ȡ���ļ�
				cnt = 0;
				for(String s : ss)
				{
					if(s.matches("[a-zA-Z]+"))
						cnt++;
				}
				minre = Double.MAX_VALUE;
				t1 = 0 - Math.log(ci + cnt);
				int i=0;
				for(Map<String , Double> m : mtrain)
				{
					td = 0;
					for(String s : ss)
					{
						if(s.matches("[a-zA-Z]+"))
						{
							if(m.containsKey(s))
							{
								td += m.get(s);
							}
							else
							{
								td += t1;
							}
						}
					}
					if(minre + td > 0)
					{				
						minre = 0 - td;
						filet = filename.get(i);						
					}
					++i;
				}
				if(filen.equals(filet))
				{
					re3.put(filen, re3.get(filen) + 1);
					re1.put(filet, re1.get(filet) + 1);
				}
				else
				{
					if(re1.containsKey(filet))
					{
						re1.put(filet, re1.get(filet) + 1);
					}
				}
			}
		}
		String tempp = new String();
		for(FileStatus fs : fileFS.listStatus(bayesTestPath))
		{
			String fname = fs.getPath().getName();
			tempp += fname + "\t\t����Ϊ" + fname + "����ĵ�����Ϊ:" + re1.get(fname).toString() + "\t\t" + 
					fname + "���ĵ���������Ϊ:" + re2.get(fname).toString() + "\t\t" + 
					"�����Գ�" + fname + "������ĵ�����Ϊ:" + re3.get(fname).toString() + "\n";
			double P = (re3.get(fname)*1.0) / re1.get(fname);
			double R = (re3.get(fname)*1.0) / re2.get(fname);
			double F = 2*P*R/(P+R);
			tempp += fname + "\t\tP=" + String.valueOf(P) + "\t\tR=" +
			String.valueOf(R) + "\t\tF=" + String.valueOf(F) + "\n\n";
			mresult.put(fname, Double.valueOf(F));
		}		
		for(String key : mresult.keySet())
		{
			System.out.println("���� "+key+" ����׼ȷ�ĸ�����: "+mresult.get(key));
			tempp += "���� " + key + " ����׼ȷ�ĸ�����: " + mresult.get(key) + "\n\n";
		}
		Path p2 = new Path(bayesTrainPath.getParent().toString() + "/PResult");
		FileSystem fs = p2.getFileSystem(conf);
		byte[] buf = null;		
		if(fs.exists(p2))
		{
			System.out.println("�ļ��Ѿ�������");
		}
		FSDataOutputStream out = fs.create(p2);
		buf = tempp.getBytes();
		out.write(buf, 0, buf.length);
		out.close();
		fs.close();
	}
}
