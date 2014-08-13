package bm.hadoop.shortestpath;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Text, Text, Text, Text> {
	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	protected void map(Text key, Text values,
			Mapper<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		System.out.println("Start node is: "+key.toString());
		System.out.println("Adjacency node minus the start node: "+values.toString());
		
		// output the i/p unchanged to preserve information
		context.write(key, values);
		
		Node node = Node.fromMR(values.toString());
		
		// only work on start node
		if(node.isDistanceSet()) {
			
			for(String adjacentNodeName: node.getAdjacentNodeNames()) {				
				outputKey.set(adjacentNodeName);
				
				int distance = node.getDistance() + 1;
				String backPtr = node.constructBackpointer(key.toString());
				
				Node outputNode = new Node();
				outputNode.setDistance(distance);
				outputNode.setBackpointer(backPtr);
				
				outputValue.set(outputNode.toString());
				
				context.write(outputKey, outputValue);
			}
		}
	}

}
