package bm.hadoop.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortMapReduce {
	private static final String SEPARATOR = "\t";
	
	static class Map extends Mapper<Text, Text, Person, Person> {
		private Person outputKey;
		private Person outputValue;
		
		@Override
		protected void map(Text key, Text values,
				Mapper<Text, Text, Person, Person>.Context context)
				throws IOException, InterruptedException {
			String[] parts = values.toString().split(SEPARATOR);
			Integer commonFriends = Integer.valueOf(parts[1]);
			outputKey = new Person(parts[0], commonFriends);
			outputValue = new Person(key.toString(), commonFriends);
			
			context.write(outputKey, outputValue);
			
			outputValue = new Person(parts[0], commonFriends);
			outputKey = new Person(key.toString(), commonFriends);
			
			context.write(outputKey, outputValue);
		}
		
	}
	
	static class Reduce extends Reducer<Person, Person, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void reduce(Person key, Iterable<Person> values,
				Reducer<Person, Person, Text, Text>.Context context)
				throws IOException, InterruptedException {
			outputKey.set(key.getName());
			
			StringBuilder output = new StringBuilder();
			int index = 0;
			for(Person p : values) {
				if(index++ >= 10) {
					break;
				}
				output.append(p.getName()).append(":").append(p.getCommonFriends()).append(",");								
			}
			if(output.indexOf(",") != -1) {
				outputValue.set(output.substring(0, output.length() -1));
			} else {
				outputValue.set(output.toString());
			}
			context.write(outputKey, outputValue);
		}
		
	}
}
