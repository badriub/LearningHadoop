package bm.hadoop.fof;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendsMapReduce extends Mapper<Text, Text, Text, Text> {
	private static final String SEPARATOR = "\t";
	
	 public static String joinPersonsLexicographically(String person1,
             String person2) {
		if (person1.compareTo(person2) < 0) {
		return person1 + SEPARATOR + person2;
		}
		return person2 + SEPARATOR + person1;
	}
	 
	static class Map extends Mapper<Text, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
	    private IntWritable two = new IntWritable(2);
	    
		@Override
		protected void map(Text key, Text values,
				Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException,
				InterruptedException {
			String currentName = key.toString();
			System.out.println(currentName);
			String[] friends = values.toString().split(SEPARATOR);
			IntWritable friendshipDistance = new IntWritable(1);
			String adjName = null;
			int index = 0;
			for(String friend : friends) {
				context.write(new Text(FriendsMapReduce.joinPersonsLexicographically(key.toString(), friend)), one);
				
				for(int innerIndex = index + 1; innerIndex < friends.length; innerIndex++) {
					context.write(new Text(FriendsMapReduce.joinPersonsLexicographically(friend, friends[innerIndex])), two);
				}
				index++;
			}
			
		}
	}

	static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			boolean alreadyFriends = false;
			IntWritable commonFriends = new IntWritable(0);
			Iterator<IntWritable> i = values.iterator();
			while(i.hasNext()) {
				IntWritable distance = i.next();
				if(distance.get() == 1) {
					alreadyFriends = true;
					break;
				}
				commonFriends.set(commonFriends.get() + 1);
			}
			
			if(!alreadyFriends) {
				context.write(key, commonFriends);
			}
			
		}
	}
}
