package org.bgi.flexlab.gaea.data.structure.memoryshare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.LineReader;

public abstract class WholeGenomeShare {
	protected static String DISTRIBUTE_CACHE_FLAG = "distribute.cache.flag";

	public static boolean distributeCache(String chrList, Job job, String cacheName)
			throws IOException, URISyntaxException {
		job.addCacheFile(new URI(chrList + "#" + cacheName));

		Configuration conf = job.getConfiguration();
		Path refPath = new Path(chrList);
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();

		String chrFile = "";
		String[] chrs = new String[3];
		while ((in.readLine(line)) != 0) {
			chrFile = line.toString();
			chrs = chrFile.split("\t");
			File fileTest = new File(chrs[1]);
			if (fileTest.isFile()) {
				chrs[1] = "file://" + chrs[1];
			}
			job.addCacheFile(new URI(chrs[1] + "#" + chrs[0]));
		}
		in.close();
		refin.close();
		return true;
	}

	protected void loadChromosomeList(String cacheName) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(cacheName)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e.toString());
		}
		
		String line = new String();
		try {
			while((line = br.readLine()) != null) {
				String[] chrs = line.split("\t");
				// insert chr
				if(!addChromosome(chrs[0])) {
					br.close();
					throw new RuntimeException("map Chromosome "+chrs[1]+" Failed.");
				}
				setChromosome(chrs[0],chrs[0],Integer.parseInt(chrs[2]));
			}
		} catch (NumberFormatException | IOException e) {
			throw new RuntimeException(e.toString());
		}
		try {
			br.close();
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}
	
	protected void loadChromosomeList(Path refPath) throws NumberFormatException, IOException{
		Configuration conf = new Configuration();
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();
		
		String chrFile = "";
		String[] chrs = new String[3];
		while((in.readLine(line)) != 0){
			chrFile = line.toString();
			chrs = chrFile.split("\t");
			
			// insert chr
			if(!addChromosome(chrs[0])) {
				in.close();
				throw new RuntimeException("map Chromosome "+chrs[1]+" Failed.");
			}
			setChromosome(chrs[1],chrs[0],Integer.parseInt(chrs[2]));
		}
		in.close();
	}

	public static void distributeCacheReference(String chrList, Job job, String cacheName, String distributeCacheFlag) {
		try {
			if (distributeCache(chrList, job, cacheName)) {
				job.getConfiguration().setBoolean(DISTRIBUTE_CACHE_FLAG + "." + cacheName, true);
			}
		} catch (IOException | URISyntaxException e) {
			throw new RuntimeException(e.toString());
		}
	}

	public boolean loadGenome(Configuration conf, String cacheName) {
		boolean isDistributeRef = conf.getBoolean(DISTRIBUTE_CACHE_FLAG + "." + cacheName, false);
		if (!isDistributeRef)
			return false;

		try {
			loadChromosomeList(cacheName);
		} catch (Exception e) {
			throw new RuntimeException(e.toString());
		}
		return true;
	}

	public boolean loadGenome(String refList) {
		try {
			loadChromosomeList(refList);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	public abstract void clean();

	public abstract boolean addChromosome(String chrName);
	
	public abstract void setChromosome(String path,String chrName,int length);
}
