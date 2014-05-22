package mapred.hashtagsim;

import java.io.IOException;
import java.util.ArrayList;
import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.InputLines;
import mapred.filesystem.CommonFileOperations;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		// getJobFeatureVector(input, tmpdir + "/job_feature_vector");

		// String jobFeatureVector = loadJobFeatureVector(tmpdir
		//		+ "/job_feature_vector");

		// System.out.println("Job feature vector: " + jobFeatureVector);

		getHashtagFeatureVector(input, tmpdir + "/feature_vector");

		// Iterate feature vectors from all temporary files and compute pairwise similarities.
		computeHashTagSimilarities(tmpdir + "/feature_vector", output);

		// getHashtagSimilarities(jobFeatureVector, tmpdir + "/feature_vector",
		// 		output);
	}

	/**
	 * Computes the word cooccurrence counts for hashtag #job
	 * 
	 * @param input
	 *            The directory of input files. It can be local directory, such
	 *            as "data/", "/home/ubuntu/data/", or Amazon S3 directory, such
	 *            as "s3n://myawesomedata/"
	 * @param output
	 *            Same format as input
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getJobFeatureVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for hashtag #Job");

		job.setClasses(JobMapper.class, JobReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(1);

		job.run();
	}

	/**
	 * Loads the computed word cooccurrence count for hashtag #job from disk.
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	private static String loadJobFeatureVector(String dir) throws IOException {
		// Since there'll be only 1 reducer that process the key "#job", result
		// will be saved in the first result file, i.e., part-r-00000
		FileUtil fu = new FileUtil();
		String job_featureVector = fu.load(dir + "/part-r-00000");

		// The feature vector looks like "#job word1:count1;word2:count2;..."
		String featureVector = job_featureVector.split("\\s+", 2)[1];
		return featureVector;
	}

	/**
	 * [Team34]Compute hashTag Similarities for all hash tags computed in all temporary files.
	 */
	private static void computeHashTagSimilarities(String input, String output) throws IOException, Exception
	{
		//String[] fileNames = CommonFileOperations.listAllFiles(input, ".*part.*", false);

		String[] fileNames = CommonFileOperations.listAllFiles(input, ".*part.*", false);

		System.out.println("[Team 34] There are " + fileNames.length + " files for hash tag features.");
		
		// JobConf.setNumTasksToExecutePerJvm(-1);
		System.out.println("[Team 34] Successfully Configured the number of tasks to run.");

		// String tmpRstDir = tmp + "/tmp_rsts";
		for (int i = 0; i < fileNames.length; i ++)
		{
			// String localFileName = tmp + "/" + "hashtag_" + i;
			// CommonFileOperations.copyFromHDFS(fileNames[i], localFileName);
			String inputNodeName = fileNames[i];

			System.out.println("[Team 34] Processing hash tags in file " + inputNodeName);
			computeNodeSimilarities(input, inputNodeName, output, i);
		}

		//String[] rstFiles = CommonFileOperations.listAllFiles(tmpRstDir, ".*part.*", true);
		//for (int j = 0; j < rstFiles.length; j ++)
		//{
		//	OutputStream out = openFileForWrite(output + "/rsts.txt");
		//}
	}

	/**
	 * Loads the computed word cooccurrence count for a group of hash tags from one node.
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	private static void computeNodeSimilarities(String input, String filename, String outputDir, int file_num) throws IOException, Exception {
		FileUtil fu = new FileUtil();
		InputLines lines = fu.loadLines(filename);
		int line_num = 0;
		for(String line:lines)
		{
			String hashTag = line.split("\\s+", 2)[0];
			String featureVec = line.split("\\s+", 2)[1];
			String outputName = outputDir + "/file" + file_num + "_hashtag" + line_num;

			Configuration conf = new Configuration();
			conf.set("featVector", featureVec);
			conf.set("hashTag", hashTag);
			//conf.setNumTasksToExecutePerJvm(-1);

			System.out.println("[Team 34] Computing hashtag similarity job for hashtag " + hashTag + " with feature vector " + featureVec);
		
			Optimizedjob job = new Optimizedjob(conf, input, outputName,
				"Get similarities between hashtag " + hashTag + " and other hashtags!");
			// Enable multiple runs on an jvm
			//job.setNumTasksToExecutePerJvm(-1);
			job.setClasses(SimilarityMapper.class, null, null);
			job.setMapOutputClasses(Text.class, IntWritable.class);
			job.run();
			line_num ++;
		}
		
	}

	/**
	 * Same as getJobFeatureVector, but this one actually computes feature
	 * vector for all hashtags.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getHashtagFeatureVector(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for all hashtags");
		job.setClasses(HashtagMapper.class, HashtagReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}

	/**
	 * When we have feature vector for both #job and all other hashtags, we can
	 * use them to compute inner products. The problem is how to share the
	 * feature vector for #job with all the mappers. Here we're using the
	 * "Configuration" as the sharing mechanism, since the configuration object
	 * is dispatched to all mappers at the beginning and used to setup the
	 * mappers.
	 * 
	 * @param jobFeatureVector
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getHashtagSimilarities(String jobFeatureVector,
			String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		// Share the feature vector of #job to all mappers.
		Configuration conf = new Configuration();
		conf.set("jobFeatureVector", jobFeatureVector);
		
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get similarities between #job and all other hashtags");
		job.setClasses(SimilarityMapper.class, null, null);
		job.setMapOutputClasses(IntWritable.class, Text.class);
		job.run();
	}
}
