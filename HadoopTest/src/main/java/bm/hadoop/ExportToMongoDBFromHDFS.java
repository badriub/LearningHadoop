package bm.hadoop;


public class ExportToMongoDBFromHDFS {

	/*public static class ReadWeblogs extends
			Mapper<ObjectWritable, Text, ObjectId, BSONObject> {

		private ObjectId text = new ObjectId();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			System.out.println("Key: " + key);
			System.out.println("Value: " + value);
			
			String[] fields = value.toString().split("\t");

			String md5 = fields[0];
			String url = fields[1];
			String date = fields[2];
			String time = fields[3];
			String ip = fields[4];

			BSONObject b = new BasicBSONObject();
			b.put("md5", md5);
			b.put("url", url);
			b.put("date", date);
			b.put("time", time);
			b.put("ip", ip);

			context.write(text, b);
		}
	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf,
				"mongodb://localhost:27017/test.weblogs");

		// MongoConfigUtil.setCreateInputSplits(conf, false);
		System.out.println("Configuration: " + conf);

		final Job job = new Job(conf, "Export to Mongo");

		Path in = new Path("/data/weblogs/weblog_entries.txt");
		FileInputFormat.setInputPaths(job, in);

		job.setJarByClass(ExportToMongoDBFromHDFS.class);
		job.setMapperClass(ReadWeblogs.class);

		job.setOutputKeyClass(ObjectId.class);
		job.setOutputValueClass(BSONObject.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}*/

}
