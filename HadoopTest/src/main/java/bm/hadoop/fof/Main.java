package bm.hadoop.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bm.hadoop.fof.PersonComparator;
import bm.hadoop.fof.PersonNameComparator;

public class Main {
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
	    String calcOutputDir = args[1];
	    String sortOutputDir = args[2];
	    
	    System.out.println("inputFile:"+inputFile);
	    System.out.println("calcOutputDir:"+calcOutputDir);
	    
	    if(runCalcJob(inputFile, calcOutputDir) ) {
	    	runSortJob(calcOutputDir, sortOutputDir);
	    }
	}

	private static boolean runSortJob(String calcOutputDir, String sortOutputDir) {
		try {
			Configuration configuration = new Configuration();
			
			Job jobConf = new Job(configuration);
			jobConf.setJarByClass(Main.class);
			
			FileInputFormat.setInputPaths(jobConf, calcOutputDir);
			Path outputPath = new Path(sortOutputDir);
			outputPath.getFileSystem(configuration).delete(outputPath, true);
			FileOutputFormat.setOutputPath(jobConf, outputPath);
			
			jobConf.setMapperClass(SortMapReduce.Map.class);
			jobConf.setReducerClass(SortMapReduce.Reduce.class);
			
			jobConf.setInputFormatClass(KeyValueTextInputFormat.class);
			
			jobConf.setMapOutputKeyClass(Person.class);
			jobConf.setMapOutputValueClass(Person.class);
		    
			jobConf.setPartitionerClass(PersonNamePartitioner.class);
			jobConf.setGroupingComparatorClass(PersonNameComparator.class);
			jobConf.setSortComparatorClass(PersonComparator.class);
			
			jobConf.setOutputKeyClass(Text.class);
			jobConf.setOutputValueClass(Text.class);
			return jobConf.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			
			throw new RuntimeException(e);
		}
		
	}

	private static boolean runCalcJob(String inputFile, String calcOutputDir) {
		try {
			Configuration configuration = new Configuration();
			
			Job jobConf = new Job(configuration);
			jobConf.setJarByClass(Main.class);
		    
			FileInputFormat.setInputPaths(jobConf, inputFile);
			Path outputPath = new Path(calcOutputDir);
			outputPath.getFileSystem(configuration).delete(outputPath, true);
			FileOutputFormat.setOutputPath(jobConf, outputPath);
			
			jobConf.setJobName("myjob");
			jobConf.setMapperClass(FriendsMapReduce.Map.class);
			jobConf.setReducerClass(FriendsMapReduce.Reduce.class);
			jobConf.setJarByClass(Main.class);
			
			jobConf.setInputFormatClass(KeyValueTextInputFormat.class);
			
			jobConf.setMapOutputKeyClass(Text.class);
			jobConf.setMapOutputValueClass(IntWritable.class);
			
			jobConf.setOutputKeyClass(Text.class);
			jobConf.setOutputValueClass(IntWritable.class);
			return jobConf.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			
			throw new RuntimeException(e);
		}
	}
}
