package bm.hadoop.sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortRunner {
	public static void main(String... args) throws Exception {
		runSortJob(args[0], args[1]);
	}

	private static void runSortJob(String input, String output) throws Exception {
		Configuration conf = new Configuration();

	    Job job = new Job(conf);
	    job.setJarByClass(SortRunner.class);
	    
	    job.setMapperClass(SortMap.class);
	    job.setReducerClass(SortReducer.class);
	    
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    
	    job.setMapOutputKeyClass(Person.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setPartitionerClass(PersonNamePartitioner.class);
	    job.setGroupingComparatorClass(PersonNameComparator.class);
	    job.setSortComparatorClass(PersonComparator.class);
	    
	    Path outputPath = new Path(output);

	    FileInputFormat.setInputPaths(job, input);
	    FileOutputFormat.setOutputPath(job, outputPath);

	    outputPath.getFileSystem(conf).delete(outputPath, true);

	    job.waitForCompletion(true);
	}
	
	public static class SortMap extends Mapper<Text, Text, Person, Text> {
		private Person outputKey = new Person();

		@Override
		protected void map(Text lastName, Text firstName,
				Mapper<Text, Text, Person, Text>.Context context)
				throws IOException, InterruptedException {
			outputKey.setFirstName(firstName.toString());
			outputKey.setLastName(lastName.toString());
			context.write(outputKey, firstName);
		}
	}
	
	public static class SortReducer extends Reducer<Person, Text, Text, Text> {
		Text lastName = new Text();

		@Override
		protected void reduce(Person person, Iterable<Text> values,
				Reducer<Person, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			lastName.set(person.getLastName());
			for (Text firstName : values) {
				context.write(lastName, firstName);
			}
		}
	}
}
