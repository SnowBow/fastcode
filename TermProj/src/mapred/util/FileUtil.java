package mapred.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;

import mapred.filesystem.CommonFileOperations;

public class FileUtil {
	static Configuration conf;
	static FileSystem fs;
	
	public FileUtil() {
		
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void setConfiguration(Configuration c) {
		conf = c;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*static {
		setConfiguration(new Configuration());
	}*/

	public void save(String str, String filename) throws IOException {
		OutputStream out = write(filename);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
		writer.write(str);
		writer.close();
	}

	// Only loads the first line
	public String load(String filename) throws IOException {
		InputStream is = read(filename);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String str = reader.readLine();
		reader.close();
		return str;
	}

	// Saves to temp dir
	public void saveTmp(String str, String filename) throws IOException {
		save(str, getTmpDir() + "/" + filename);
	}

	// Saves to temp dir
	public String loadTmp(String filename) throws IOException {
		return load(getTmpDir() + "/" + filename);
	}

	// Saves to temp dir
	public void saveInt(int i, String name) {
		try {saveTmp(Integer.toString(i), name);} catch (IOException e) {e.printStackTrace();}
	}
	
	// Loads from temp dir
	public Integer loadInt(String name) {
		try {return Integer.parseInt(loadTmp(name));} catch (IOException e) {e.printStackTrace();return null;}
	}
	
	// Saves to temp dir
	public void saveLong(long i, String name) {
		try {saveTmp(Long.toString(i), name);} catch (IOException e) {e.printStackTrace();}
	}
	
	// Loads from temp dir
	public Long loadLong(String name) {
		try {return Long.parseLong(loadTmp(name));} catch (IOException e) {e.printStackTrace();return null;}
	}
	
	// Saves to temp dir
	public void saveDouble(Double i, String name) {
		try {saveTmp(Double.toString(i), name);} catch (IOException e) {e.printStackTrace();}
	}
	
	// Loads from temp dir
	public Double loadDouble(String name) {
		try {return Double.parseDouble(loadTmp(name));} catch (IOException e) {e.printStackTrace();return null;}
	}

	// Loads all lines
	public InputLines loadLines(String filename) throws IOException {
		InputStream is = read(filename);
		return new InputLines(is);
	}

	public OutputStream write(String filename) throws IOException {
		fs = FileSystem.get(URI.create(filename), conf);
		CommonFileOperations.deleteIfExists(filename);
		return fs.create(new Path(filename));
	}

	public InputStream read(String filename) throws IOException {
		fs = FileSystem.get(URI.create(filename), conf);
		return fs.open(new Path(filename));
	}

	public String getTmpDir() {
		return conf.get("hadoop.tmp.dir");
	}
	

	public FileSystem getFS() {
		return fs;
	}

	public static void testFileOperation() throws IOException {
		FileUtil fu = new FileUtil();
		fu.save("abc\n", "data/abc");
		String str = fu.load("data/abc");
		System.out.println(str);
		for (String s : fu.loadLines("data/abstract.tiny.test"))
			System.out.println(s);
	}
}
