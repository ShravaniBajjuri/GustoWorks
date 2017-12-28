package com.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import com.MAPPER.WCMapper;
import com.REDUCER.WCReducer;
/*import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;

	import com.WC.Mapper.*;
	import com.WC.Reducer.*;

 * */
public class WCDriver extends Configured {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//main
//step-0
//validating  input arguments
		if(args.length<2)
		{
			System.out.println("Java Usage :" +WCDriver.class.getName()+ " [config] /hdfs/path/to/input /hdfs/path/to/output");			
		}
		
		//Loading the configuration create conf object
		Configuration conf =new Configuration(Boolean.TRUE);
		
		//call ToolRunner Generic Option Parser parsing command line arguments and set the configuration
		
		try
		{
			//int i =ToolRunner.run(conf, new WCDriver(), args);
			int i=ToolRunner.run(conf,new WCDriver() ,args);
			//(conf,new WCDriver(),args);
			//(conf, new WCDriver(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
			else
			{
				System.out.println("FAILURE");
			}
		}
		catch(Exception e)
		{
			System.out.println("FAILURE");
			e.printStackTrace();
		}
		
	}
	//Override
	public int run(String[] args) throws Exception
	{
		//Step-1
		//Get configuration object
		Configuration conf = super.getConf();
		
		//step-2
		//setting parameters to configuration object
		//creating job instance to access cluster environment
		//created wordcountJb instance & calling conf and setting to the WCDriver class
		Job wordCountJob = Job.getInstance(conf, WCDriver.class.getName());	
		
		//Setting Jar class Path
		wordCountJob.setJarByClass(WCDriver.class);
		
		//setting input
		final String input= args[0];
		
		//converting string to URI
		final Path hdfsInputPath=new Path(input);
		
		//Will be useful fo converting job submitter / application master
		//datatypes we declaring here in text Input Format
		
		TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		//setting output
		final String output= args[1];
		
		//converting string to URI
		final Path hdfsOutputPath= new Path(output);
		
		//Setting output format
		//output format
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOutputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		//setting mapper
		wordCountJob.setMapperClass(WCMapper.class);
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		//setting reducer
		wordCountJob.setReducerClass(WCReducer.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		
		//trigger method
		wordCountJob.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}
	

}
