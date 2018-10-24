import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONObject;

public class MapReduce {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text disease = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String tweet = value.toString();
				String[] tuple = tweet.split("\\n");

				for (int i = 0; i < tuple.length; i++) {
					JSONObject jsonObj = new JSONObject(tuple[i]);
					String tw = (String) ((JSONObject) jsonObj.get("extended_tweet")).get("full_text");
					Long id = (Long) jsonObj.get("id");
					tw = tw.toLowerCase();
					
					if (tw.contains("flu")) {
						disease.set("flu");
						context.write(disease, new Text(Long.toString(id)));
					}else if (tw.contains("zika")) {
						disease.set("zika");
						context.write(disease, new Text(Long.toString(id)));
					}else if (tw.contains("diarrhea")) {
						disease.set("diarrhea");
						context.write(disease, new Text(Long.toString(id)));
					}else if (tw.contains("ebola")) {
						disease.set("ebola");
						context.write(disease, new Text(Long.toString(id)));
					}else if (tw.contains("swamp")) {
						disease.set("swamp");
						context.write(disease, new Text(Long.toString(id)));
					}else if (tw.contains("change")) {
						disease.set("change");
						context.write(disease, new Text(Long.toString(id)));
					}

				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String ids = "";
			for (Text val : values) {
				ids = ids + val.toString() + ", ";
			}
			result.set(ids);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MapReduce.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}