package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Filedel {

	/**
	 * @param args
	 * @throws IOException 
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(new Path("/venkat"))){
			fs.delete(new Path("/venkat"));
			System.out.println("file deleted");
		}

	}

}
