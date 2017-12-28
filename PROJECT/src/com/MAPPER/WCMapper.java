package com.MAPPER;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text , Text, IntWritable>
{

	@Override
	protected void map(LongWritable key, Text value, Context context)throws java.io.IOException, InterruptedException
	{
		//TODO Business Logic-1
		//key--0
		//Value--DEER BEAR RIVER
		//Long Lkey=key.get();
		final String strValue=value.toString();
		
		//strValue=DEER BEAR RIVER
		if(!StringUtils.isEmpty(strValue))
		{
			final String[] words= StringUtils.splitPreserveAllTokens(strValue, "");
		    //words[0]=DEER
			//words[1]=BEAR
			final Text outputWord= new Text();
			final IntWritable ONE = new IntWritable(1);
			for(String word : words)
			{
				outputWord.set(word);
				context.write(outputWord, ONE);
			}
		}
	}
}
