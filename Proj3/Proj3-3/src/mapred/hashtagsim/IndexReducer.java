package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {		
		
		StringBuilder builder = new StringBuilder();	
		for (Text word : value) {
			String w = word.toString();
			builder.append(w);
		}
		
		context.write(key, new Text(builder.toString()));
	}
}
