package bm.hadoop.shortestpath;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static final String TARGET_NODE = "shortestpath.targetnode";
	
	public static void main(String[] args) throws Exception {
		String startNode = args[0];
	    String targetNode = args[1];
	    String inputFile = args[2];
	    String outputDir = args[3];
	    
	    Path inputPath = createMapReduceInputData(startNode, inputFile, outputDir);
	    runJob(inputPath, startNode, targetNode, outputDir);
	}

	private static void runJob(Path inputPath, String startNode,
			String targetNode, String outputDir) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    Path outputPath = new Path(outputDir);
	    int iter = 1;

	    while (true) {

	      Path jobOutputPath =
	          new Path(outputPath, String.valueOf(iter));

	      System.out.println("======================================");
	      System.out.println("=  Iteration:    " + iter);
	      System.out.println("=  Input path:   " + inputPath);
	      System.out.println("=  Output path:  " + jobOutputPath);
	      System.out.println("======================================");

	      if(findShortestPath(inputPath, jobOutputPath, startNode, targetNode)) {
	        break;
	      }
	      inputPath = jobOutputPath;
	      iter++;
	    }
	}

	private static boolean findShortestPath(Path inputPath,
            Path outputPath, String startNode, String targetNode)
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration);
		
		job.setJarByClass(Main.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
		
		if(!job.waitForCompletion(true)) {
			throw new RuntimeException("Job failed");
		}
		
		Counter counter = (Counter) job.getCounters().findCounter(Reduce.PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
		if(counter != null && counter.getValue() > 0) {
	      CounterGroup group = job.getCounters().getGroup(Reduce.PathCounter.PATH.toString());
	      Iterator<Counter> iter = group.iterator();
	      iter.hasNext();
	      String path = iter.next().getName();
	      System.out.println("==========================================");
	      System.out.println("= Shortest path found, details as follows.");
	      System.out.println("= ");
	      System.out.println("= Start node:  " + startNode);
	      System.out.println("= End node:    " + targetNode);
	      System.out.println("= Hops:        "  + counter.getValue());
	      System.out.println("= Path:        "  + path);
	      System.out.println("==========================================");
	      return true;
	    }
	    return false;
	}

	/**
	 * Method will create new input file in the o/p directory
	 * format will be node di
	 * @param startNode
	 * @param inputFile
	 * @param outputDir
	 * @return
	 * @throws IOException
	 */
	private static Path createMapReduceInputData(String startNode,
			String inputFile, String outputDir) throws IOException {
		Configuration conf = new Configuration();
		Path originalInputPath = new Path(inputFile);
	    FileSystem fs = originalInputPath.getFileSystem(conf);
	    
	    Path outputPath = new Path(outputDir);
//	    outputPath.getFileSystem(conf).delete(outputPath, true);
//	    outputPath.getFileSystem(conf).mkdirs(outputPath);
	    
	    Path inputPathForMapReduce = new Path(outputPath, "inputForMapReduce.txt");
	    OutputStream outputStream = fs.create(inputPathForMapReduce);
	    
	    LineIterator lineIterator = IOUtils.lineIterator(fs.open(originalInputPath), "UTF-8");
	    while(lineIterator.hasNext()) {
	    	String line = lineIterator.nextLine();
	    	String[] parts = StringUtils.split(line);
	    	
	    	StringBuilder output = new StringBuilder(parts[0] + '\t');
	    	int distance = Node.INFINITE;
	    	if(parts[0].equalsIgnoreCase(startNode)) {
	    		distance = 0;
	    	}
	    	output.append(distance + '\t');
	    	output.append(StringUtils.join(parts, '\t', 1, parts.length));
	    	
	    	IOUtils.write(output, outputStream);
	    }
	    outputStream.close();
	    
		return inputPathForMapReduce;
	}

}
