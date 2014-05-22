package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * We compute the similarities between two hashtags for one word.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String tagPair = "";
		String[] invertedIndex = line.split("\\s+", 2);

		String word = invertedIndex[0];
		Map<String, Integer> indicesCount = parseIndexVector(invertedIndex[1]);

		ArrayList<String> keys = new ArrayList(indicesCount.keySet());
		for (int i = 0; i < keys.size(); i ++)
			for (int j = i; j < keys.size(); j ++)
			{
				String tag1 = keys.get(i);
				String tag2 = keys.get(j);
				Integer sum = indicesCount.get(tag1) * indicesCount.get(tag2);
				if (tag1.compareTo(tag2) >= 0)
					tagPair = tag2 + "\t" + tag1;
				else
					tagPair = tag1 + "\t" + tag2;
				context.write(new Text(tagPair), new IntWritable(sum));
			}
	}

        /**
         * De-serialize the feature vector into a map
         * 
         * @param featureVector
         *            The format is "word1:count1;word2:count2;...;wordN:countN;"
         * @return A HashMap, with key being each word and value being the count.
         */
        private Map<String, Integer> parseIndexVector(String indexVector) {
                Map<String, Integer> indexMap = new HashMap<String, Integer>();
                String[] indices = indexVector.split(";");
                for (String tag : indices) {
                        String[] word_count = tag.split(":");
                        indexMap.put(word_count[0], Integer.parseInt(word_count[1]));
                }
                return indexMap;
        }

}

