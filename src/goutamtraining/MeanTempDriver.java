package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MeanTempDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/home/cloudera/Desktop/localhadoop/meantemp_out"),true);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(TempMapper.class);
		job.setCombinerClass(TempReducerMean.class);
		job.setReducerClass(TempReducerMean.class);
		
		FileInputFormat.addInputPath(job,new Path("/home/cloudera/Desktop/localhadoop/meantemp"));
		FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/localhadoop/meantemp_out"));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
