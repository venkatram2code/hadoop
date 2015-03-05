package goutamtraining;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReducerMean extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		int mean = 0, sum = 0, i =0 ;
		while(value.iterator().hasNext()){
			
			sum += value.iterator().next().get();
			i++;
		
		}
		mean = sum/i;
		context.write(key, new IntWritable(mean));
	}
	
	

}
