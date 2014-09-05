package bm.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	public void map(Text key, Text values, OutputCollector<Text, Text> outputCollector,
			Reporter reporter) throws IOException {
		// preserve graph structure
		outputCollector.collect(key, values);
		
		Node node = Node.fromMR(values.toString());
		
		if(node.getAdjacentNodeNames() != null && 
				node.getAdjacentNodeNames().length > 0) {
			double outboundPageRank = node.getPageRank()/ (double) node.getAdjacentNodeNames().length;
			
			for(String adjNodeName : node.getAdjacentNodeNames()) {
				outputKey.set(adjNodeName);
				
				Node outNode = new Node();
				outNode.setPageRank(outboundPageRank);
				outputValue.set(outNode.toString());
				
				outputCollector.collect(outputKey, outputValue);
			}
		}
	}

}
