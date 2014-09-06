package bm.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private static final double DAMPING_FACTOR = 0.85;
	  public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
	static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
	
	private double sumPageRank;
	
	private int numberOfNodesInGraph;
	private Text outValue = new Text();

	public static enum Counter {
		CONV_DELTAS
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		numberOfNodesInGraph = context.getConfiguration().getInt(
				CONF_NUM_NODES_GRAPH, 0);
	}

	public void reduce(Text key, Iterator<Text> iterator,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		Node originalNode = null;
		while(iterator.hasNext()) {
			Node node = Node.fromMR(iterator.next().toString());
			if(node.containsAdjacentNodes()) {
				originalNode = node;
			} else {
				sumPageRank+= node.getPageRank();
			}
		}
		
		double dampingFactor =
		        ((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);
		
		double pageRank = dampingFactor + DAMPING_FACTOR * sumPageRank;
		double delta = originalNode.getPageRank() - pageRank;

	    originalNode.setPageRank(delta);
	    
	    outValue.set(originalNode.toString());
	    
	    collector.collect(key, outValue);
	    
		int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));

		System.out.println("Delta = " + scaledDelta);
		org.apache.hadoop.mapred.Counters.Counter counter = reporter.getCounter(Counter.CONV_DELTAS);
		counter.increment(scaledDelta);
	}

}
