package mapred.ngramcount;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		// Added by chenw-0314-2014
		int N = parser.getInt("n");

		getJobFeatureVector(input, output, N);

	}

	private static void getJobFeatureVector(String input, String output, int N)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("n", Integer.toString(N));
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Compute NGram Count ");

		job.setClasses(NgramCountMapper.class, NgramCountReducer.class, null);
		job.setMapOutputClasses(Text.class, NullWritable.class);

		job.run();
	}	
}
