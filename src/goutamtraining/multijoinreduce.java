package goutamtraining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class multijoinreduce extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	//111;US;2014 from text
	//111;65 from seq
	// country;year;temp
	@Override
	protected void reduce(IntWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String tmp = null, tmp2 = null;
		
		while(value.iterator().hasNext()){
			tmp = value.iterator().next().toString();
			tmp2 = value.iterator().next().toString();
			if(tmp.compareTo(tmp2) >= 0)
				tmp = tmp+";"+tmp2;
			else
				tmp = tmp2+";"+tmp;
		}
		context.write(null, new Text(tmp));
	}
		

}
