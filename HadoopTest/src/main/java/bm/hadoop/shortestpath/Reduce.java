package bm.hadoop.shortestpath;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<Text, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	private String targetNode = null;
	public static enum PathCounter {
		TARGET_NODE_DISTANCE_COMPUTED,
	    PATH
	}	
	  
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		outputKey.set(key);
		
		int minDistance = Node.INFINITE;
		String shortestPath = null;
		Iterator<Text> iterator = values.iterator();
		while(iterator.hasNext()) {
			Node node = Node.fromMR(iterator.next().toString());
			
			if(node.getDistance() < minDistance) {
				minDistance = node.getDistance();
				shortestPath = node.getBackpointer();
			}
		}
		Node outputNode = new Node();
		outputNode.setDistance(minDistance);
		outputNode.setBackpointer(shortestPath);
		outputValue.set(outputNode.toString());
		
		context.write(outputKey, outputValue);
		
		if (minDistance != Node.INFINITE &&
		        targetNode.equals(key.toString())) {
		      Counter counter = context.getCounter(
		          PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
		      counter.increment(minDistance);
		      context.getCounter(PathCounter.PATH.toString(),
		    		  outputNode.getBackpointer()).increment(1);
		    }
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		targetNode = context.getConfiguration().get(Main.TARGET_NODE);
	}

}
