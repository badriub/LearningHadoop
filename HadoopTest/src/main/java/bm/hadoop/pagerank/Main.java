package bm.hadoop.pagerank;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;


public class Main {

	public static void main(String... args) throws Exception {
		String inputFile = args[0];
		String outputDir = args[1];

		iterate(inputFile, outputDir);
	}

	private static void iterate(String inputFile, String outputDir) {
		Path outputPath = new Path(outputDir);
		
		Configuration conf = new Configuration();
		createOutputDirectory(outputPath, conf);
		
	    Path inputPath = new Path(outputPath, "input.txt");
	    
	    long numberOfNodesInGraph = createInputFile(new Path(inputFile), inputPath);
	    int iter = 1;
	    double desiredConvergence = 0.01;

	    while (true) {

	      Path jobOutputPath =
	          new Path(outputPath, String.valueOf(iter));

	      System.out.println("======================================");
	      System.out.println("=  Iteration:    " + iter);
	      System.out.println("=  Input path:   " + inputPath);
	      System.out.println("=  Output path:  " + jobOutputPath);
	      System.out.println("======================================");

	      if (calcPageRank(inputPath, jobOutputPath, numberOfNodesInGraph) <
	          desiredConvergence) {
	        System.out.println(
	            "Convergence is below " + desiredConvergence +
	                ", we're done");
	        break;
	      }
	      inputPath = jobOutputPath;
	      iter++;
	    }
	}

	private static double calcPageRank(Path inputPath, Path outputPath,
			long numberOfNodesInGraph) {
		Configuration configuration = new Configuration();
		configuration.setLong(Reduce.CONF_NUM_NODES_GRAPH.toString(), numberOfNodesInGraph);
		
		JobConf jobConf = new JobConf(configuration);
		FileInputFormat.setInputPaths(jobConf, inputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		jobConf.setJarByClass(Main.class);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		long summedConvergence = 0;

		try {
			RunningJob job = JobClient.runJob(jobConf);
			if(job.getCounters().findCounter("bm.hadoop.pagerank.Reduce$Counter", Reduce.Counter.CONV_DELTAS.toString()) != null) {
				summedConvergence = job.getCounters().findCounter("bm.hadoop.pagerank.Reduce$Counter", Reduce.Counter.CONV_DELTAS.toString()).getValue();
			}
		} catch (IOException e) {
			throw new RuntimeException("Job failed", e);
		}
		
		double convergence =
		        ((double) summedConvergence /
		            Reduce.CONVERGENCE_SCALING_FACTOR) /
		            (double) numberOfNodesInGraph;

	    System.out.println("======================================");
	    System.out.println("=  Num nodes:           " + numberOfNodesInGraph);
	    System.out.println("=  Summed convergence:  " + summedConvergence);
	    System.out.println("=  Convergence:         " + convergence);
	    System.out.println("======================================");
		    
		return convergence;
	}

	private static long createInputFile(Path inputPath, Path outputPath) {
		Configuration conf = new Configuration();
		long numberOfNodes = 0;
		try {
			FileSystem fs = inputPath.getFileSystem(conf);
			numberOfNodes = IOUtils.readLines(fs.open(inputPath), "UTF8").size();
			
			OutputStream os = fs.create(outputPath);
		    LineIterator iter = IOUtils
		        .lineIterator(fs.open(inputPath), "UTF8");
			while(iter.hasNext()) {
				String line = iter.nextLine();
				System.out.println(line);
				String[] parts = StringUtils.split(line);
				
				Node node = new Node();
				node.setPageRank(1.0/numberOfNodes);	
				node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
				IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
			}
			os.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return numberOfNodes;
	}

	private static void createOutputDirectory(Path outputPath,
			Configuration conf) {
		try{
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    outputPath.getFileSystem(conf).mkdirs(outputPath);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
