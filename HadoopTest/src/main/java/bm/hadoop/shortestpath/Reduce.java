package bm.hadoop.shortestpath;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private Text outValue = new Text();
	private String targetNode = null;
	public static enum PathCounter {
		TARGET_NODE_DISTANCE_COMPUTED,
	    PATH
	}	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		int minDistance = Node.INFINITE;

	    System.out.println("input -> K[" + key + "]");

	    Node shortestAdjacentNode = null;
	    Node originalNode = null;

	    for (Text textValue : values) {
	      System.out.println("  input -> V[" + textValue + "]");

	      Node node = Node.fromMR(textValue.toString());

	      if(node.containsAdjacentNodes()) {
	        // the original data
	        //
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
	    context.write(key, outValue);
		/*outputKey.set(key);
		
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
		
		context.write(outputKey, outputValue);*/
		
		System.out.println("***************************************************");
		System.out.println("Min Distance="+minDistance);
		System.out.println("targetNode="+targetNode);
		System.out.println("key="+key.toString());
		System.out.println("***************************************************");
		
		if (minDistance != Node.INFINITE &&
		        targetNode.equalsIgnoreCase(key.toString())) {
			System.out.println("Setting the minDistance counter");
			org.apache.hadoop.mapred.Counters.Counter counter = (org.apache.hadoop.mapred.Counters.Counter) context.getCounter(
		          PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
		      counter.setValue(minDistance);
		      System.out.println("set min dist is ="+counter.getValue());
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
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		targetNode = context.getConfiguration().get(Main.TARGET_NODE);
	}

}
