package com.lei.hadoop.countword;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lei.hadoop.util.HadoopUtil;


/**
 * 
 * 
 * @author stones333
 *
 */

// com.lei.hadoop.countword.SortWords
public class SortWords extends Configured implements Tool {

	/**
	 * 
	 * @author stones333
	 *
	 */
	public static class SortWordsMap extends MapReduceBase 
		implements Mapper<LongWritable, Text, IntWritable, Text> {
		@Override
		public void map(LongWritable key, Text value, 
				OutputCollector<IntWritable, Text> collector, 
				Reporter reporter) throws IOException 
		{
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);

			int number = 0; 
			String word = "";

			if(stringTokenizer.hasMoreTokens())
			{
				String strWord= stringTokenizer.nextToken();
				word = strWord.trim();
			}

			if( stringTokenizer.hasMoreElements())
			{
				String strNum = stringTokenizer.nextToken();
				number = Integer.parseInt(strNum.trim());
			}

			collector.collect(new IntWritable(number), new Text(word));

		}
	}

	/**
	 * 
	 * @author stones333
	 *
	 */
	public static class SortWordsReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
	    {
	        while((values.hasNext()))
	        {
	        	output.collect(key, values.next());
	        }

	    }
	}

	
	

	public int run(String[] args) throws Exception {
		runSortWords(args);
		return 0;
	}
	
	public int runSortWords(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), SortWords.class);
		conf.setJobName("wordsort");
		 	
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		 	
		conf.setMapperClass(SortWordsMap.class);
		conf.setCombinerClass(SortWordsReduce.class);
		conf.setReducerClass(SortWordsReduce.class);
		 	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		 	
		HadoopUtil.removeDirectory(conf, args[1]);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 	
		JobClient.runJob(conf);

		HadoopUtil.copyHDFSDirToLocal (conf, args[1], args[1] ) ;
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SortWords(), args);
		System.exit(res);
	}

}
