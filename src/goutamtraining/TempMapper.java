package goutamtraining;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends Mapper<Text, Text, Text, IntWritable> {
	
	private Text country = new Text();
	private IntWritable temp = new IntWritable();


	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//input is US;2012;65
		String str = value.toString();
		StringTokenizer st = new StringTokenizer(str, ";");
		while(st.hasMoreTokens()){
			country.set(key);
			st.nextToken();
			Integer i = Integer.parseInt(st.nextToken());
			temp.set(i);
			context.write(country, temp);
			
		}
		
		
	}

}
