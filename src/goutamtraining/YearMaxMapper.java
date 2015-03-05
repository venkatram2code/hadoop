package goutamtraining;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearMaxMapper extends Mapper<Text, Text, Text, Text> {
	
	private Text country = new Text();
	private Text remain = new Text();
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		while(context.nextKeyValue()){
			
			country.set(key);
			remain.set(value);
			
			context.write(country, remain);
			
			System.out.println(country+"\t"+remain);
			
			
		}
	}
	
	

}
