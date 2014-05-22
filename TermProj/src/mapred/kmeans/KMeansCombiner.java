package mapred.kmeans;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

	protected void reduce(IntWritable key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {		
		
		ArrayList<Double> summed_feat = new ArrayList<Double>();
		ArrayList<Double> cur_feat = new ArrayList<Double>();
		int count = 0;
		for (Text word : value) {
			String feat = word.toString();
			cur_feat = parseFeatureVector(feat);
			if (count == 0)
				summed_feat = (ArrayList<Double>)cur_feat.clone();
			else
				summed_feat = AddArrays(summed_feat, cur_feat);
			count++;
		}
		
		String summed_feat_str = Integer.toString(count) + ":";
		for (Double f : summed_feat)
		{
			summed_feat_str = summed_feat_str + f.toString() + " ";
		}
		// System.out.println("[Team34-KMeans] The summed feature vector for key " + key + " is : " + summed_feat_str);
		// System.out.println("[Team34-KMeans] The total number of features for key " + key + " is : " + count);
		
		// summed_feat = generalizeArray(summed_feat, count);
		
		/*
		 * We're serializing the clusters into string
		 */
		// String generalized_feat_str = "";
		// for (Double f : summed_feat)
		// {
		// 	generalized_feat_str = generalized_feat_str + f.toString() + "\t";
		// }

		System.out.println("[Team34-KMeans] The summed center on each node: " + summed_feat_str);
		
		context.write(key, new Text(summed_feat_str));
	}

	/**
	 * @param summed_feat
	 * @param cur_feat
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<Double> AddArrays(ArrayList<Double> summed_feat,
			ArrayList<Double> cur_feat) {
		ArrayList<Double> summation = new ArrayList<Double>();
		summation = (ArrayList<Double>) summed_feat.clone();
		
		Integer array_sz = summation.size();
		if (!array_sz.equals(cur_feat.size()))
				array_sz = array_sz > cur_feat.size() ? cur_feat.size() : array_sz;
		
		for (int i = 0; i < array_sz; i ++)
			summation.set(i, summation.get(i) + cur_feat.get(i));
		
		return summation;
	}


	/**
	 * @param featureString: the string of data vector
	 * @return the vector of data
	 */
	private ArrayList<Double> parseFeatureVector(String featureString) {
		ArrayList<Double> feature = new ArrayList<Double>();
		String[] feature_data = featureString.split("\\s+");
		for (String dat : feature_data) {
			feature.add(Double.parseDouble(dat));
		}
		return feature;
	}
}
