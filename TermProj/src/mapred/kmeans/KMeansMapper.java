package mapred.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	Map<Integer, ArrayList<Double>> centers = null;

	/**
	 * We compute the inner product of feature vector of every hashtag with that
	 * of #job
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data_str = line.split("\\s+", 2);

		ArrayList<Double> feat = parseFeatureVector(data_str[1]);
		
		Integer feat_class_id = findNearestCenter(centers, feat);

		context.write(new IntWritable(feat_class_id), new Text(data_str[1]));
	}

	/**
	 * This function is ran before the mapper actually starts processing the
	 * records, so we can use it to setup the job feature vector.
	 * 
	 * Loads the feature vector for hashtag #job into mapper's memory
	 */
	@Override
	protected void setup(Context context) {
		String centers_str = context.getConfiguration().get("centers");
		centers = parseCenters(centers_str);		
	}


	/**
	 * @param centers_str
	 * @return 
	 */
	private Map<Integer, ArrayList<Double>> parseCenters(String centers_str) {
		Map<Integer, ArrayList<Double>> center_data = new HashMap<Integer, ArrayList<Double>>();
		String[] centers = centers_str.split("\n");
		for (String center : centers)
		{
			String[] center_records = center.split("\\s+", 2);
			int center_id = Integer.parseInt(center_records[0]);
			ArrayList<Double> feat = parseFeatureVector(center_records[1]);
			center_data.put(center_id, feat);
		}
		
		return center_data;
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


	/**
	 * @param centers_dat: the array of each center
	 * @param feat: the vector of one point of data
	 * @return
	 */
	private Integer findNearestCenter(
			Map<Integer, ArrayList<Double>> centers2, ArrayList<Double> feat) {
		// TODO Auto-generated method stub
		Integer minCenterID = 0;
		double euclidean_dist = Double.MAX_VALUE;
		double cur_dist = euclidean_dist;
		
		for (Map.Entry<Integer, ArrayList<Double>> center_entry : centers2.entrySet())
		{
			cur_dist = computeEuclideanDist(center_entry.getValue(), feat);
			if(cur_dist < euclidean_dist)
			{
				minCenterID = center_entry.getKey();
				euclidean_dist = cur_dist;
			}
		}
		
		return minCenterID;
	}
	
	/**
	 * @param arrayList
	 * @param feat
	 * @return
	 */
	private double computeEuclideanDist(ArrayList<Double> arrayList,
			ArrayList<Double> feat) {
		// TODO Auto-generated method stub
		double dist = 0.0;
		Integer featSize = arrayList.size();
		if (!featSize.equals(feat.size()))
		{
			featSize = (arrayList.size() > feat.size()? feat.size():arrayList.size());
		}
		
		for (int i = 0; i < featSize; i ++)
		{
			dist += (arrayList.get(i) - feat.get(i)) * (arrayList.get(i) - feat.get(i));
		}
		
		return dist;
	}
}


