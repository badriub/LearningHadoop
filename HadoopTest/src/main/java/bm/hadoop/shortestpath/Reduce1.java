package bm.hadoop.shortestpath;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private String targetNode = null;
	private Text outValue = new Text();
	public static enum PathCounter {
		TARGET_NODE_DISTANCE_COMPUTED,
	    PATH
	}	
	
	public void reduce(Text key, Iterator<Text> iterator,
			OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
		int minDistance = Node.INFINITE;

	    System.out.println("input -> K[" + key + "]");

	    Node shortestAdjacentNode = null;
	    Node originalNode = null;

	    while(iterator.hasNext()) {
	    	Text textValue = iterator.next();
		      System.out.println("  input -> V[" + textValue + "]");

		      Node node = Node.fromMR(textValue.toString());

		      if(node.containsAdjacentNodes()) {
		        originalNode = node;
		      }

		      if(node.getDistance() < minDistance) {
		        minDistance = node.getDistance();
		        shortestAdjacentNode = node;
		      }	    	
	    }

	    if(shortestAdjacentNode != null) {
	      originalNode.setDistance(minDistance);
	      originalNode.setBackpointer(shortestAdjacentNode.getBackpointer());
	    }

	    outValue.set(originalNode.toString());

	    System.out.println(
	        "  output -> K[" + key + "],V[" + outValue + "]");
	    outputCollector.collect(key, outValue);
		
		System.out.println("***************************************************");
		System.out.println("Min Distance="+minDistance);
		System.out.println("targetNode="+targetNode);
		System.out.println("key="+key.toString());
		System.out.println("***************************************************");
		
		if (minDistance != Node.INFINITE &&
		        targetNode.equalsIgnoreCase(key.toString())) {
			System.out.println("Setting the minDistance counter");
			org.apache.hadoop.mapred.Counters.Counter counter = reporter.getCounter(PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
//			org.apache.hadoop.mapred.Counters.Counter counter = (org.apache.hadoop.mapred.Counters.Counter) context.getCounter(
//		          PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
		      counter.increment(minDistance);
		      System.out.println("set min dist is ="+counter.getValue());
		      reporter.getCounter(PathCounter.PATH.toString(),
		              shortestAdjacentNode.getBackpointer()).increment(1);
		      
//		      context.getCounter(PathCounter.PATH.toString(),
//		    		  shortestAdjacentNode.getBackpointer()).increment(1);
			      /*Counter counter = context.getCounter(
			          PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
		          counter.increment(minDistance);
		          context.getCounter(PathCounter.PATH.toString(),
		              shortestAdjacentNode.getBackpointer()).increment(1);*/
		    }
	}

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		targetNode = job.get(Main.TARGET_NODE);
		System.out.println("Target node is set to: "+targetNode);
	}

}
