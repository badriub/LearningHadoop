package bm.hadoop.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
	    String calcOutputDir = args[1];
	    
	    System.out.println("inputFile:"+inputFile);
	    System.out.println("calcOutputDir:"+calcOutputDir);
	    
	    runCalcJob(inputFile, calcOutputDir);
	}

	private static void runCalcJob(String inputFile, String calcOutputDir) {
//		RunningJob job = JobClient.runJob(jobConf);
		try {
			Configuration configuration = new Configuration();
			
			Job jobConf = new Job(configuration);
			jobConf.setJarByClass(Main.class);
		    
		    
			//JobConf jobConf = new JobConf(configuration);
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
			jobConf.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			
			throw new RuntimeException(e);
		}
	}
}
