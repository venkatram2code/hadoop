package goutamtraining;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeqMapper extends Mapper<IntWritable, Text, Text, Text> {
	
	private Text con = new Text();
	private Text ysno = new Text();
	
	@Override
	protected void map(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//2014;US;111 to  US 2014;111
		System.out.println(key+"\t"+value);
		String[] str = value.toString().split(";");
		con.set(str[1]);
		ysno.set(str[0]+";"+str[2]);
		context.write(con,ysno);
		
	}
		
}
