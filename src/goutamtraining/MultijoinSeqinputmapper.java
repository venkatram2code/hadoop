package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultijoinSeqinputmapper extends Mapper<IntWritable, Text, IntWritable, Text> {
	
	//Sequencefileinputformat
	//111;65
	private IntWritable sno = new IntWritable();
	private Text temp = new Text();
	@Override
	protected void map(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String[] str = value.toString().split(";");
		sno.set(Integer.parseInt(str[0]));
		temp.set(str[1]);
		context.write(sno, temp);
		
	}
	
	

}
