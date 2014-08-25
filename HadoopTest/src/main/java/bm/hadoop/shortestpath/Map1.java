package bm.hadoop.shortestpath;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map1 extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	public void map(Text key, Text values, OutputCollector<Text, Text> outputCollector,
			Reporter reporter) throws IOException {
		System.out.println("Start node is: "+key.toString());
		System.out.println("Adjacency node minus the start node: "+values.toString());
		
		// output the i/p unchanged to preserve information
		outputCollector.collect(key, values);
//		context.write(key, values);
		
		Node node = Node.fromMR(values.toString());
		
		// only work on start node
		if(node.isDistanceSet()) {
			int distance = node.getDistance() + 1;
			String backPtr = node.constructBackpointer(key.toString());
			
			for(String adjacentNodeName: node.getAdjacentNodeNames()) {				
				outputKey.set(adjacentNodeName);
				
				Node outputNode = new Node();
				outputNode.setDistance(distance);
				outputNode.setBackpointer(backPtr);
				
				outputValue.set(outputNode.toString());
				
//				context.write(outputKey, outputValue);
				outputCollector.collect(outputKey, outputValue);
			}
		}
	
		
	}

}
