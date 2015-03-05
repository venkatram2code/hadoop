package seqchapter;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class Multiijoinseq {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String urinfile = "/home/cloudera/Desktop/localhadoop/multijoin/";
		String uroutfile = "/home/cloudera/Desktop/localhadoop/multijoin/text2.seq";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uroutfile), conf);
		Path path = new Path(uroutfile);

		IntWritable key = new IntWritable();
		Text value = new Text();

		File infile = new File(urinfile);
		SequenceFile.Writer writer = null;
		try{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			int i = 1;
			for(String line : FileUtils.readLines(infile)){
				key.set(i++);
				value.set(line);
				writer.append(key, value);
				System.out.println(key+"\t"+value);
			}
		}
		finally{
			writer.close();
		}


	}

}
