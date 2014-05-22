package mapred.hashtagsim;

import java.io.IOException;

import mapred.util.Tokenizer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] hashtag_featureVector = line.split("\\s+", 2);

		String hashtag = hashtag_featureVector[0];
		Map<String, Integer> features = parseFeatureVector(hashtag_featureVector[1]);

		/*
		 * Iterate all words, find out all hashtags, then iterate all other non-hashtag 
		 * words and map out.
		 */
		for (Map.Entry<String, Integer> feat : features.entrySet()) 
			context.write(new Text(feat.getKey()), new Text(hashtag + ":" + feat.getValue() + ";"));
	}

	/**
         * De-serialize the feature vector into a map
         * 
         * @param featureVector
         *            The format is "word1:count1;word2:count2;...;wordN:countN;"
         * @return A HashMap, with key being each word and value being the count.
         */
        private Map<String, Integer> parseFeatureVector(String featureVector) {
                Map<String, Integer> featureMap = new HashMap<String, Integer>();
                String[] features = featureVector.split(";");
                for (String feature : features) {
                        String[] word_count = feature.split(":");
                        featureMap.put(word_count[0], Integer.parseInt(word_count[1]));
                }
                return featureMap;
        }
}
