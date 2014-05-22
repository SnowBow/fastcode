package mapred.kmeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;

import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.InputLines;
import mapred.filesystem.CommonFileOperations;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);
        System.out.println("driver accessed");
		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");
		int cluster_num = Integer.valueOf(parser.get("cluster_num"));
		double th = Double.parseDouble(parser.get("th"));
        System.out.println("input:"+input+"\n output:"+output+"\n tmpdir:"+tmpdir);
		// Initialize the cluster center as the first cluster_num numbers of data feature.
        String centers = initCenters(input, cluster_num, tmpdir + "/oldCenters.dat");
        System.out.println("centers: "+centers);
		kmeansClustering(centers, input, output, tmpdir, th);
		
		// double cen_diff = compareCenters(tmpdir + "oldCenters.dat", tmpdir + "/newCenters");
	}

	/**
	 * @param input: input folder for data.
	 * @param cluster_num: the number of clusters for kmeans algorithm
	 * @throws IOException
	 */
	private static String initCenters(String input, int cluster_num, String filename) throws IOException {
		
		String centers = "";
		int n = 0;
		InputLines lines;
		FileUtil fu = new FileUtil();
		
		System.out.println("Initializing centers for map reduce task!!");
		lines = fu.loadLines(input);
		Iterator<String> linesIter = lines.iterator();
		
		while ((n < cluster_num) && (linesIter.hasNext()))
		{	
			String curCenter = linesIter.next();
			centers = centers + curCenter + "\n";
			// System.out.println("The " + n + " center is : \n" + curCenter);
			n ++;
		}
		
		fu.save(centers, filename);
		
		return centers;
	}
	

	/**
	 * @param clusters : initialized cluster centers.
	 * @param input : input data folder
	 * @param output : output clusters folder
	 * @param tmpdir : temporary clusters folder
	 * @param th 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private static void kmeansClustering(String centers, String input,
			String output, String tmpdir, double th) throws IOException, ClassNotFoundException, InterruptedException {
		
		int i = 1;
		updateCenters(input, centers, tmpdir + "/newCenters");
		
		System.out.println("The new centers have been updated!");
		
		double cen_diffs = compareCenters(tmpdir + "/oldCenters.dat", tmpdir + "/newCenters");
		
		System.out.println("[Team34-KMeans] The difference of old centers and new centers is : " + Double.toString(cen_diffs));
		
		while (cen_diffs > th)
		{
			centers = readCenterString(tmpdir + "/oldCenters.dat");
			updateCenters(input, centers, tmpdir + "/newCenters");
			cen_diffs = compareCenters(tmpdir + "/oldCenters.dat", tmpdir + "/newCenters");
			i ++;
			System.out.println("[Team34-KMeans] The difference of old centers and new centers is : " + Double.toString(cen_diffs));
			System.out.println("[Team34-KMeans] Interation Number : " + i);
		}
		
		CommonFileOperations.copyToHDFS(tmpdir + "/newCenters", output);
	}

	/**
	 * @param input
	 * @param clusters
	 * @param string
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private static void updateCenters(String input, String centers, String output) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		CommonFileOperations.deleteIfExists(output);
		Configuration conf = new Configuration();
		conf.set("centers", centers);
		
		System.out.println("[Team 34 - MapReduce KMeans] Classifying data and update centers ");
		
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Update the centers of kmeans algorithm");
			// Enable multiple runs on an jvm
			//job.setNumTasksToExecutePerJvm(-1);
		job.setClasses(KMeansMapper.class, KMeansReducer.class, KMeansCombiner.class);
		job.setMapOutputClasses(IntWritable.class, Text.class);
		job.run();
	}


	/**
	 * @param oldFileName: the file name of old centers
	 * @param newDirName: the directory name of new centers
	 * @return
	 * @throws IOException 
	 */
	private static double compareCenters(String oldFileName, String newDirName) throws IOException {
		// TODO Auto-generated method stub
		Map<Integer, ArrayList<Double>> oldCenters = readCenters(oldFileName);
		Map<Integer, ArrayList<Double>> newCenters = readCenters(newDirName);
		
		Integer centerNum = oldCenters.size();
		if (!centerNum.equals(newCenters.size()))
		{
			System.err.println("[team34-KMeans]The new center number does not equal to the old center number.");
		}
		
		double diff = 0;
		for (int i = 1; i <= centerNum; i ++)
		{
			ArrayList<Double> cen1 = oldCenters.get(i);
			ArrayList<Double> cen2 = newCenters.get(i);
			
			diff += getDiff(cen1, cen2);
		}
		
		writeCenterFile(newCenters, oldFileName);
		return diff;
	}

	/**
	 * @param newCenters
	 * @param oldFileName
	 * @throws IOException
	 */
	private static void writeCenterFile(
			Map<Integer, ArrayList<Double>> newCenters, String oldFileName) throws IOException {
		CommonFileOperations.deleteIfExists(oldFileName);
		
		String centers = "";
		FileUtil fu = new FileUtil();
		for (Map.Entry<Integer, ArrayList<Double>> e : newCenters.entrySet())
		{
			centers = centers + e.getKey().toString() + "\t";
			for (Double dat : e.getValue())
			{
				centers = centers + dat.toString() + "\t";
			}
			centers = centers + "\n";
		}
		
		fu.save(centers, oldFileName);
				
	}

	/**
	 * @param cen1
	 * @param cen2
	 * @return
	 */
	private static double getDiff(ArrayList<Double> cen1,
			ArrayList<Double> cen2) {
		double diff = 0.0;
		Integer dim = cen1.size();
		if (!dim.equals(cen2.size()))
		{
			dim = (dim > cen2.size() ? cen2.size() : dim);
		}
		
		for (int i = 0; i < dim; i ++)
		{
			diff += (cen1.get(i) - cen2.get(i))*(cen1.get(i) - cen2.get(i));
		}
		
		return diff;
	}

	/**
	 * @param pathName
	 * @return
	 * @throws IOException
	 */
	private static Map<Integer, ArrayList<Double>> readCenters(String pathName) throws IOException {
		// TODO Auto-generated method stub
		Map<Integer, ArrayList<Double>> centers = new HashMap<Integer, ArrayList<Double>>(); 
		FileUtil fu = new FileUtil();
		if (!CommonFileOperations.isFileTrue(pathName))
		{
			String[] fileNames = CommonFileOperations.listAllFiles(pathName, ".*part.*", false);
			for (int i = 0; i < fileNames.length; i ++)
			{
				InputLines lines = fu.loadLines(fileNames[i]);
				for (String line:lines)
				{
					String[] center_records = line.split("\\s+", 2);
					int center_id = Integer.parseInt(center_records[0]);
					ArrayList<Double> feat = parseFeatureVector(center_records[1]);
					centers.put(center_id, feat);
				}
			}
		}
		else
		{
			InputLines lines = fu.loadLines(pathName);
			for (String line:lines)
			{
				String[] center_records = line.split("\\s+", 2);
				int center_id = Integer.parseInt(center_records[0]);
				ArrayList<Double> feat = parseFeatureVector(center_records[1]);
				centers.put(center_id, feat);
			}
		}
		
		return centers;
	}
	
	/**
	 * @param string
	 * @return
	 * @throws IOException 
	 */
	private static String readCenterString(String pathName) throws IOException {
		// TODO Auto-generated method stub
		String centersString = "";
		FileUtil fu = new FileUtil();
		
		if (!CommonFileOperations.isFileTrue(pathName))
		{
			String[] fileNames = CommonFileOperations.listAllFiles(pathName, ".*part.*", false);
			for (int i = 0; i < fileNames.length; i ++)
			{
				InputLines lines = fu.loadLines(fileNames[i]);
				for (String line:lines)
				{
					centersString = centersString + line + "\n";
				}
			}
		}
		else
		{
			InputLines lines = fu.loadLines(pathName);
			for (String line:lines)
			{
				centersString = centersString + line + "\n";
			}
		}
		return centersString;
	}
	
	/**
	 * @param featureString: the string of data vector
	 * @return the vector of data
	 */
	private static ArrayList<Double> parseFeatureVector(String featureString) {
		ArrayList<Double> feature = new ArrayList<Double>();
		String[] feature_data = featureString.split("\\s+");
		for (String dat : feature_data) {
			feature.add(Double.parseDouble(dat));
		}
		return feature;
	}


}
