package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bgi.flexlab.gaea.tools.vcf.sort.VCFSortOptions;
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
	private Path file;
	private final Map<Long, VCFHeader> headerID;
	private final Map<String, Long> chrOrder;
	private VCFHeader vcfHeader;
	
	public VCFRecordReader(VCFSortOptions options) {
		// TODO Auto-generated constructor stub
		headerID = options.getHeaderID();
		chrOrder = options.getChrOrder();
	}
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException{
		conf = ctx.getConfiguration();
		FileSplit split = (FileSplit) inputSplit;
		start = split.getStart();
		this.length = split.getLength();
		file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream is = fs.open(file);
	    
		reader = new AsciiLineReader(is);
	    it = new AsciiLineReaderIterator(reader);
	   
	    Object header =  codec.readHeader(it);
	    if (!(header instanceof FeatureCodecHeader) || !(((FeatureCodecHeader)header).getHeaderValue() instanceof VCFHeader)) {
			throw new IOException("No VCF header found in "+ file);
	    }	    
	    vcfHeader = (VCFHeader)(((FeatureCodecHeader)header).getHeaderValue());
	    
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
		String[] lineSplits = line.split("\t");
		String chr = lineSplits[0];
		long chrID = chrOrder.get(chr);
		long pos = Long.parseLong(lineSplits[1]);
		for(long id : headerID.keySet()) {
			if(vcfHeader.getSampleNamesInOrder().equals(headerID.get(id).getSampleNamesInOrder())){
				key.set(id + chrID + pos);
			}
		}
		value.set(codec.decode(line)); 
	  
		return true;
	}
}