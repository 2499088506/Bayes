package com.homework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class WholeInputFormat1 extends InputFormat<LongWritable, Text>{

	@Override
	public List<InputSplit> getSplits(JobContext context) 
			throws IOException, InterruptedException 
	{
	    Path[] paths = getInputPaths(context);
	    List<InputSplit> splits = new ArrayList<InputSplit>();
	    
	    long len = 0;
//	    for(Path pa : paths)
//	    {
//	    	FileSystem fileFS = pa.getFileSystem(context.getConfiguration());
//	    	for(FileStatus fs : fileFS.listStatus(pa))
//	    		len += fs.getLen();
//	    }
//	    splits.add(new FileSplit(paths[0], 0, len, new String[0]));
		FileSystem fileFS = paths[0].getFileSystem(context.getConfiguration());
	    
		for(Path pa : paths)
	    {
			len = 0;
			for(FileStatus f : fileFS.listStatus(pa))
		    {
		    	len += f.getLen();
		    }
			splits.add(new FileSplit(pa, 0, len, new String[0]));
	    }
		
    	 
	    return splits; 
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		MyInputRecordReader1 reader = new MyInputRecordReader1();
		reader.initialize(split, context);
		return reader;
	}
	
	public static Path[] getInputPaths(JobContext context) 
	{
		String dirs = context.getConfiguration().get("mapred.input.dir", "");
	    String [] list = StringUtils.split(dirs);
	    Path[] result = new Path[list.length];
	    for (int i = 0; i < list.length; i++) 
	    {
	      result[i] = new Path(StringUtils.unEscapeString(list[i]));
	    }
	    return result;
	}
	
}

class MyInputRecordReader1 extends RecordReader<LongWritable, Text>
{
	private FileSplit filesplit;
	private Configuration conf;
	private Text value = new Text();
	private LongWritable key = new LongWritable();
	private boolean processed = false;
	private FileStatus paths[];
	private int index = 0;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) 
			throws IOException, InterruptedException 
	{
		this.filesplit = (FileSplit)split;
		this.conf = context.getConfiguration();
		paths = filesplit.getPath().getFileSystem(conf).listStatus(filesplit.getPath());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException 
	{
		if(index >= paths.length)
			return false;
		byte[] contents = new byte[(int) paths[index].getLen()];
		FileSystem fs = paths[index].getPath().getFileSystem(conf);
		FSDataInputStream in = null;
		try{
			in = fs.open(paths[index].getPath());
			IOUtils.readFully(in, contents, 0, contents.length);
			value.set(contents, 0, contents.length);
//			System.out.println("·ÖÆ¬Â·¾¶:" + paths[index].getPath() + "   " + filesplit.getPath());
		}finally{
			IOUtils.closeStream(in);
		}
		++index;
		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
