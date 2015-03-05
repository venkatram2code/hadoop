package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultiInputJoinDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/home/cloudera/Desktop/localhadoop/multijoin/out"),true);
				
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
				
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path("/home/cloudera/Desktop/localhadoop/multijoin/text"), TextInputFormat.class,MultijoinTextinputmapper.class);
		MultipleInputs.addInputPath(job, new Path("/home/cloudera/Desktop/localhadoop/multijoin/text2.seq"), SequenceFileInputFormat.class,MultijoinSeqinputmapper.class);
		job.setReducerClass(multijoinreduce.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/localhadoop/multijoin/out"));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
