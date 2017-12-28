package com.REDUCER;

import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	protected void reduce(Text key,java.lang.Iterable<IntWritable> values,Context context) throws java.io.IOException, InterruptedException 
	{
		//TODO Business logic - 2
		
		//key ---- BEAR 
		//VALUES --- {2,1,1,4,1}
		int sum=0;
		for (IntWritable value : values) {
			sum=sum+value.get();
		}
		context.write(key, new IntWritable(sum));
		
		
		//BEAR , 2+1+1+4+1 = 9
	};


}
