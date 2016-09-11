package com.homework;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CombineWholeFileInputFormat extends CombineFileInputFormat<NullWritable, Text>{

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) throws IOException 
	{
		CombineFileSplit combinefilesplit = (CombineFileSplit)split;
		MyWholeRecordReader recordReader = new MyWholeRecordReader();
		try{
			recordReader.initialize(combinefilesplit, context);
		}catch(InterruptedException e){
			new RuntimeException("Error to initialize CombineSmallfileRecordReader.");
		}
		return recordReader;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// TODO Auto-generated method stub
		return super.getSplits(job);
	}
	
}
class MyWholeRecordReader extends RecordReader<NullWritable, Text>
{
	private FileSplit filesplit;
	private Configuration conf;
	private Text value = new Text();
	private boolean processed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) 
			throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		this.filesplit = (FileSplit)split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		if(!processed)
		{
			byte[] contents = new byte[(int) filesplit.getLength()];
			Path file = filesplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try{
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			}finally{
				IOUtils.closeStream(in);
			}
			processed = true;
			return processed;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
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

