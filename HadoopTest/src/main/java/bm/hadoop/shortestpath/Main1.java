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
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;

public class Main1 {
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
		configuration.set(TARGET_NODE, targetNode);
		
		JobConf jobConf = new JobConf(configuration);
		FileInputFormat.setInputPaths(jobConf, inputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		
		jobConf.setJobName("myjob");
		jobConf.setMapperClass(Map1.class);
		jobConf.setReducerClass(Reduce1.class);
		jobConf.setJarByClass(Main.class);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		RunningJob job = JobClient.runJob(jobConf);
		long counter = 0;
		if(job.getCounters().findCounter("bm.hadoop.shortestpath.Reduce1$PathCounter", Reduce.PathCounter.TARGET_NODE_DISTANCE_COMPUTED.toString()) != null) {
			counter = job.getCounters().findCounter("bm.hadoop.shortestpath.Reduce1$PathCounter", Reduce.PathCounter.TARGET_NODE_DISTANCE_COMPUTED.toString()).getValue();
		}
		if(counter > 0 ) {
			  Group group = job.getCounters().getGroup(Reduce.PathCounter.PATH.toString());
		      Iterator<org.apache.hadoop.mapred.Counters.Counter> iter = group.iterator();
		      iter.hasNext();
		      String path = iter.next().getName();
	      System.out.println("==========================================");
	      System.out.println("= Shortest path found, details as follows.");
	      System.out.println("= ");
	      System.out.println("= Start node:  " + startNode);
	      System.out.println("= End node:    " + targetNode);
	      System.out.println("= Hops:        "  + counter);
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
			    	    
	    Path outputPath = new Path(outputDir);
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    outputPath.getFileSystem(conf).mkdirs(outputPath);
	    
	    Path inputPathForMapReduce = new Path(outputPath, "inputForMapReduce.txt");
	    
	    
	    FileSystem fs = inputPathForMapReduce.getFileSystem(conf);
	    OutputStream outputStream = fs.create(inputPathForMapReduce);
	    
	    Path originalInputPath = new Path(inputFile);
	    
	    FileSystem originalInputFS = inputPathForMapReduce.getFileSystem(conf);
	    conf = new Configuration();
	    System.out.println( originalInputFS.exists(originalInputPath));
	    LineIterator lineIterator = IOUtils.lineIterator(originalInputFS.open(originalInputPath), "UTF-8");
	    while(lineIterator.hasNext()) {
	    	String line = lineIterator.nextLine();
	    	String[] parts = StringUtils.split(line);
	    	
	    	int distance = Node.INFINITE;
	    	if(parts[0].equalsIgnoreCase(startNode)) {
	    		distance = 0;
	    	}
	    	/*output.append(distance + '\t');
	    	output.append('\t');
	    	output.append(StringUtils.join(parts, '\t', 1, parts.length));
	    	
	    	IOUtils.write(output, outputStream);*/
	    	IOUtils.write(parts[0] + '\t' + String.valueOf(distance) + "\t\t",
	    			outputStream);
			IOUtils.write(StringUtils.join(parts, '\t', 1, parts.length), outputStream);
			IOUtils.write("\n", outputStream);
	    }
	    outputStream.close();
	    
		return inputPathForMapReduce;
	}

}
