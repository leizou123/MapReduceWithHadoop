package com.lei.hadoop.countword;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lei.hadoop.util.HadoopUtil;

/**
 * 
 * @author stones333
 *
 */


// hadoop jar ./target/MapReduceWithHadoop-1.0-SNAPSHOT.jar com.lei.hadoop.countword.CountWordsV2 ./wordcount_input ./wordcount_output   
public class CountWordsV2 extends Configured implements Tool {
	
	private static String tempDir = "./temp"; 
	
	public static class CountWordsV2Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> { 
		static enum Counters { INPUT_WORDS }
		 	
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		 	
		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		 	
		private long numRecords = 0;
		private String inputFile;
		 	
		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");
		 	
			if (job.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				} catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}
		
		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		// Maper to send <word, 1> to OutputCollector
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			for (String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}
			 	
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}
			 	
			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}


	public static class CountWordsV2Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		// Reducer to sum up word count, and send them to  OutputCollector
		public void reduce(Text key, Iterator<IntWritable> values, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public int run(String[] args) throws Exception {
		
		String patternFile = null;
		List<String> argList = new ArrayList<String>();
		for (int i=0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				patternFile = args[++i];
			} else {
				argList.add(args[i]);
			}
		}
		String [] newArgs  = argList.toArray(new String[argList.size()]);

		if (newArgs.length<2) {
			System.out.println("Invalid input format!  " );
			System.out.println("The correct command should be in the format of " );
			System.out.println("$ hadoop jar ./target/MapReduceWithHadoop-1.0-SNAPSHOT.jar com.lei.hadoop.countword.CountWordsV2 ./wordcount_input ./wordcount_output  " );
			return -1;
		}
		
		int res1 = runWordCount(newArgs, patternFile);
		
		
		newArgs[0] = tempDir;
		int res2 = ToolRunner.run(new Configuration(), new SortWords(), newArgs);
		return 0;
	}
	
	public int runWordCount(String[] args, String patternFile) throws Exception {
		JobConf conf = new JobConf(getConf(), CountWordsV2Map.class);
		conf.setJobName("wordcount");
		 	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		 	
		conf.setMapperClass(CountWordsV2Map.class);
		conf.setCombinerClass(CountWordsV2Reduce.class);
		conf.setReducerClass(CountWordsV2Reduce.class);
		 	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (patternFile!=null && patternFile.length()>0) {
			DistributedCache.addCacheFile(new Path(patternFile).toUri(), conf);
			conf.setBoolean("wordcount.skip.patterns", true);
		}
		

		HadoopUtil.copyLocalDirToHDFS (conf, args[0], args[0]) ;		 	
		HadoopUtil.removeDirectory(conf, tempDir);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));

		FileOutputFormat.setOutputPath(conf, new Path(tempDir));
		 	
		JobClient.runJob(conf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CountWordsV2(), args);
		System.exit(res);
	}
}

	
	

 


