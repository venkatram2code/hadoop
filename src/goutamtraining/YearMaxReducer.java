package goutamtraining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YearMaxReducer extends Reducer<Text, Text, Text, IntWritable> {

}
