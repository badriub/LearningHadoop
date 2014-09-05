package bm.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private static final double DAMPING_FACTOR = 0.85;
	private static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
	
	private double sumPageRank;
	
	public void reduce(Text key, Iterator<Text> iterator,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
	
	}

}
