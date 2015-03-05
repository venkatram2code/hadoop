package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Math;

public class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	

	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		// TODO Auto-generated method stub
		int highest =0;
		while(value.iterator().hasNext()){
			
			highest = Math.max(highest, value.iterator().next().get());
		}
		context.write(key, new IntWritable(highest));
	}

}
