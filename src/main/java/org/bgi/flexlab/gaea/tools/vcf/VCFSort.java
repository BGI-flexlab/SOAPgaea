package org.bgi.flexlab.gaea.tools.vcf;

import java.awt.image.SampleModel;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.bgi.flexlab.gaea.format.vcf.header.GaeaSingleVCFHeader;

public class VCFSort {
	
	public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {	
		@Override
		public void map( LongWritable id, Text record, Context context) throws IOException, InterruptedException {
			context.write(id, record);
		}
	}
	
	public static class SortReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		@Override
		public void reduce( LongWritable id, Iterable<Text> records, Context context) throws IOException, InterruptedException {
			for (Text rec : records) {
				context.write(NullWritable.get(), rec);
			}
		}
	}
	
	public static class SortOutputFormat extends TextOutputFormat<NullWritable, Text> {
		@Override 
		public Path getDefaultWorkFile(TaskAttemptContext context, String ext) throws IOException {
			String extension = ext.isEmpty() ? ext : "." + ext;
			int part = context.getTaskAttemptID().getTaskID().getId();
			return new Path(context.getConfiguration().get("mapred.output.dir"),"part-" + String.format("%05d", part) + extension);
		}
	
		// Allow the output directory to exist.
		@Override 
		public void checkOutputSpecs(JobContext job) {}
	}
	
	public static GaeaSingleVCFHeader getVcfHeader(Path inputPath, Configuration conf) {
		GaeaSingleVCFHeader vcfHeader = new GaeaSingleVCFHeader();
		try {
			FileSystem fs = inputPath.getFileSystem(conf);
			fs = inputPath.getFileSystem(conf);
			if (!fs.exists(inputPath)) {
				throw new IOException("Input File Path is not exist! Please check input var.");
			}
			if (fs.isFile(inputPath)) {
				if(validPath(inputPath, fs)){
					vcfHeader.readSingleHeader(inputPath, conf);
				}	
			} else {		
				FileStatus stats[]=fs.listStatus(inputPath);
				for (FileStatus file : stats) {
					Path filePath = file.getPath();
					if (!fs.isFile(filePath)) {
						getVcfHeader(filePath, conf);
					} else if (validPath(filePath, fs)){
						vcfHeader.readSingleHeader(filePath, conf);
					}
				}
			}
			fs.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return vcfHeader;
	}
	
	private static boolean validPath(Path inputPath, FileSystem fs) throws IOException {
		return (!inputPath.getName().startsWith("_")) && (fs.getFileStatus(inputPath).getLen() != 0);
	}
	
	private static boolean run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Path inPath = new Path(args[0]);
		Path tmpPath = new Path(args[1]);
		final Timer t = new Timer();
		int reducerTaskNum = 2;
		if(args.length == 5) {
			reducerTaskNum = Integer.parseInt(args[4]);
		}
		Configuration conf = new Configuration();
		setConf(conf, args);
		
		tmpPath = tmpPath.getFileSystem(conf).makeQualified(tmpPath);
		Path partition = tmpPath.getFileSystem(conf).makeQualified(new Path(tmpPath, "_partitioning" + "vcf"));

		TotalOrderPartitioner.setPartitionFile(conf, partition);
		URI partitionURI = new URI(partition.toString() + "#" + partition.getName());
		if (!partitionURI.getScheme().equals("file")) {
			DistributedCache.addCacheFile(partitionURI, conf);
			DistributedCache.createSymlink(conf);
		}
		
		GaeaSingleVCFHeader vcfHeader = getVcfHeader(inPath, conf);
			
		Job job = new Job(conf);
		job.setJarByClass(VCFSort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setInputFormatClass(SortInputFormat.class);
		job.setOutputFormatClass(SortOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, tmpPath);
		job.setNumReduceTasks(reducerTaskNum);
		
		sample(job, reducerTaskNum);		
		//run job
		job.submit();
		System.out.println("sort :: Waiting for job completion...");
		t.start();
		if (!job.waitForCompletion(true)) {
			System.err.println("sort :: Job failed.");
			return false;
		}
		System.out.printf("sort :: Job complete in %d.%03d s.\n", t.stopS(), t.fms());
	
		return copyToLocal(args, vcfHeader, t, tmpPath, conf);
	}
	
	private static boolean copyToLocal(String[] args, GaeaSingleVCFHeader vcfHeader, Timer t, Path tmpPath, Configuration conf) throws IOException{
		if(args[2] != null && args[2] != "") {
			Path outPath = new Path(args[2]);
			
			FileSystem srcFS = tmpPath.getFileSystem(conf);
		    FileSystem dstFS = outPath.getFileSystem(conf);
		    OutputStream outs = dstFS.create(outPath);
		    
		    t.start();
		    outs.write(vcfHeader.getHeaderInfoWithSample(null).getBytes());
		    FileStatus[] parts = srcFS.globStatus(new Path(args[1], "part-[0-9][0-9][0-9][0-9][0-9]*"));
		    int i = 0;
			final Timer t2 = new Timer();
			for (final FileStatus part : parts) {
				System.out.printf("sort :: Merging part %d (size %d)...",
						            ++i, part.getLen());
				System.out.flush();

				t2.start();

				final InputStream ins = srcFS.open(part.getPath());
				IOUtils.copyBytes(ins, outs, conf, false);
				ins.close();

				System.out.printf(" done in %d.%03d s.\n", t2.stopS(), t2.fms());
			}
			for (final FileStatus part : parts) {
				srcFS.delete(part.getPath(), false);
			}
			outs.close();

			System.out.printf("sort :: Merging complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());
		} else {
			System.err.println("wrong output file name");
			return false;
		}
		
		return true;
	}
	
	private static void sample(Job job, int reducerTaskNum) throws ClassNotFoundException, IOException, InterruptedException{
		Timer t = new Timer();
		System.out.println("sort :: Sampling...");
		t.start();
		InputSampler.Sampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable,Text>(0.01, 10000, Math.max(2, reducerTaskNum));
		InputSampler.writePartitionFile(job, sampler);
		System.out.printf("sort :: Sampling complete in %d.%03d s.\n", t.stopS(), t.fms());
	}
	
	private static void setConf(Configuration conf, String[] args){
		conf.set("tmpOutputPath", args[1]);
		conf.set("referenceFAI", args[3]);
		conf.set(VCFRecordReader.CHR_INPUT_PROP, args[3]);
	}
	
	public static void main(String[] args) {
		if(args.length < 4) {
			System.err.println("./hadoop jar VcfSort.jar inputPath tmpPath ouputPath ref.fai reducerNum");
			System.exit(1);
		}
		
		try {
			if(!run(args)) {
				System.exit(1);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
}
