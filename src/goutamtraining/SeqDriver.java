package goutamtraining;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SeqDriver {

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
		fs.delete(new Path("/home/cloudera/Desktop/localhadoop/seqchapter/readseq_out"),true);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SeqMapper.class);
		
		FileInputFormat.addInputPath(job,new Path("/home/cloudera/Desktop/localhadoop/seqchapter/readseq_inter"));
		FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/localhadoop/seqchapter/readseq_out"));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
