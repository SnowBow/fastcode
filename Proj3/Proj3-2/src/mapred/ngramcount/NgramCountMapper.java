package mapred.ngramcount;
import java.io.*;
import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

		//for (String word : words)
		//	context.write(new Text(word), NullWritable.get());

		Configuration conf = context.getConfiguration();
		int N = Integer.parseInt(conf.get("n"));
		for (int i = 0; i < words.length - (N - 1); i ++)
		{
			String keyString = words[i];
			for (int j = 1; j < N; j ++)
			{
				keyString = keyString + " " + words[i + j];
			}
			context.write(new Text(keyString), NullWritable.get());
		}

	}
}
