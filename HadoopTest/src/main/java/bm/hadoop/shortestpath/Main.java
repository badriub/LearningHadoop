package bm.hadoop.shortestpath;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {
	public static final String TARGET_NODE = "shortestpath.targetnode";
	
	public static void main(String[] args) throws Exception {
		String startNode = args[0];
	    String targetNode = args[1];
	    String inputFile = args[2];
	    String outputDir = args[3];
	    
	    Path inputPath = createMapReduceInputData(startNode, inputFile, outputDir);
	}

	/**
	 * Method will create new input file in the o/p directory
	 * format will be node distance rest of nodes for each line
	 * @param startNode
	 * @param inputFile
	 * @param outputDir
	 * @return new input file
	 * @throws IOException
	 */
	private static Path createMapReduceInputData(String startNode,
			String inputFile, String outputDir) throws IOException {
		Configuration conf = new Configuration();
		Path originalInputPath = new Path(inputFile);
	    FileSystem fs = originalInputPath.getFileSystem(conf);
	    
	    Path inputPathForMapReduce = new Path(outputDir, "inputForMapReduce.txt");
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
