package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultijoinTextinputmapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	//textinputformat
		//111;US;2014
	private IntWritable sno = new IntWritable();
	private Text conyear = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String[] str = value.toString().split(";");
		sno.set(Integer.parseInt(str[0]));
		conyear.set(str[1]+";"+str[2]);
		context.write(sno, conyear);
		
	}	

}
