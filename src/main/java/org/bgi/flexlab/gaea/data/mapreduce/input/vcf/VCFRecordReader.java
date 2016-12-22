package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class VCFRecordReader extends RecordReader<LongWritable, VariantContextWritable> {
	private Configuration conf;
	
	private VCFCodec codec = new VCFCodec();
	private AsciiLineReaderIterator it;
	private AsciiLineReader reader;
	
	private final LongWritable key = new LongWritable();
	private final VariantContextWritable value = new VariantContextWritable();
	
	private long start;
	private long length;
	private long currentPos;
	
	private MultipleVCFHeader mVcfHeader;
	private Path file;
	private int fileID;

	private Map<String, Long> chrOrder;
	
	private boolean sort;
	
	public static final String CHR_ORDER_PROPERTY = "chrOrder";
//  used for reading multiple vcf file but not proceed sorting
	public VCFRecordReader(Configuration conf, boolean sort) {
		this.conf = conf;
		mVcfHeader = new MultipleVCFHeader();
		mVcfHeader.loadVcfHeader(false, conf);
		this.sort = sort;
	}
	
//	used for sorting multiple vcf file
	public VCFRecordReader(Configuration conf, String chrOrderMapObjLoc, boolean sort){
		this(conf,sort);
		chrOrder = deserialize(chrOrderMapObjLoc);
	}
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException{
		conf = ctx.getConfiguration();
		FileSplit split = (FileSplit) inputSplit;
		start = split.getStart();
		this.length = split.getLength();
		file = split.getPath();
		fileID = mVcfHeader.getId(file.toString());
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream is = fs.open(file);
	    
		reader = new AsciiLineReader(is);
	    it = new AsciiLineReaderIterator(reader);
	   
	    Object header =  codec.readHeader(it);
	    if (!(header instanceof FeatureCodecHeader) || !(((FeatureCodecHeader)header).getHeaderValue() instanceof VCFHeader)) {
			throw new IOException("No VCF header found in "+ file);
	    }	    
	    
	    if(start != 0) {
	    	is.seek(start - 1);
	    	reader = new AsciiLineReader(is);
	    	reader.readLine();
	    	it = new AsciiLineReaderIterator(reader);
	    } else {
	    	currentPos = it.getPosition();
	    	is.seek(0);
	    	reader = new AsciiLineReader(is);
	    	it = new AsciiLineReaderIterator(reader);
	    	while(keepReading(it, currentPos)){
	    		it.next();
	    	}
	    		    	
	    	if (!it.hasNext() || it.getPosition() > currentPos) {
	    		throw new IOException("Empty vcf file " + file);
	    	}
	    }
	}
	
	private boolean keepReading(AsciiLineReaderIterator it, long currentPos){
		return it.hasNext() && it.getPosition() <= currentPos && it.peek().startsWith("#");
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Long> deserialize(String chrOrderMapObj) {
		Map<String, Long> chrOrder = null;
		try{
			ObjectInputStream is = new ObjectInputStream(HdfsFileManager.getInputStream(
					new Path(chrOrderMapObj), conf));
			try{
				chrOrder = (HashMap<String, Long>)is.readObject();
				is.close();
			} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
		}catch(IOException e){
			throw new RuntimeException(e);
		}	
		return chrOrder;
	}
	
	@Override 
	public void close(){ reader.close();}
	
	@Override 
	public float getProgress() {
		return length == 0 ? 1 : (float) reader.getPosition() / length;
	}
	
	@Override 
	public LongWritable getCurrentKey()  { return key;}
	
	@Override 
	public VariantContextWritable getCurrentValue()  { return value;}
	
	@Override 
	public boolean nextKeyValue() throws IOException {
		if( !it.hasNext() || it.getPosition() > length) {
			return false;
		}
		final String line = it.next();
		if(sort) {
			String[] lineSplits = line.split("\t");
			String chr = lineSplits[0];
			long chrID = chrOrder.get(chr);
			long pos = Long.parseLong(lineSplits[1]);
			key.set(fileID << 40 + chrID + pos);
		} else {
			key.set(fileID);
		}
		value.set(codec.decode(line)); 
	  
		return true;
	}
}