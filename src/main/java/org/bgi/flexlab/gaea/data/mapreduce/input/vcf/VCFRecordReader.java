package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

class VCFRecordReader extends RecordReader<LongWritable, Text> {
	private Configuration conf;
	public static final String CHR_INPUT_PROP = "hadoopVCF.sort.output";
	
	private VCFCodec codec = new VCFCodec();
	private AsciiLineReaderIterator it;
	private AsciiLineReader reader;
	
	private final LongWritable key = new LongWritable();
	private final Text value = new Text();
	private long start;
	private long length;
	private long currentPos;
	
	
	Random randomGenerator = new Random();

	private Map<String, Integer> sampleID = new HashMap<String, Integer>();
	private static Map<String, Integer> chrOrder = new HashMap<String, Integer>();
		
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException{
		conf = ctx.getConfiguration();
		FileSplit split = (FileSplit) inputSplit;
		start = split.getStart();
		this.length = split.getLength();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream is = fs.open(file);
	    
		reader = new AsciiLineReader(is);
	    it = new AsciiLineReaderIterator(reader);
	   
	    final Object h = codec.readHeader(it);
	    if (! foundHeader(h)) {
			throw new IOException("No VCF header found in "+ file);
	    }	    

	    getChrOrder();

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
	    	
	    	getSampleID();
	    	
	    	if (!it.hasNext() || it.getPosition() > currentPos) {
	    		throw new IOException("Empty vcf file " + file);
	    	}
	    }
	    
	}
	
	private void getSampleID() {
		sampleID.clear();
    	String[] cols2 = it.next().split("\t");
    	List<String> fields = Arrays.asList(cols2);
    	List<String> samples = fields.subList(9, fields.size());
    	for(int j = 1; j <= samples.size(); j++) {
    		sampleID.put(samples.get(j-1), j);
    	}
	}
	
	private void getChrOrder() throws IOException {
		chrOrder.clear();
        Path chrOrderPath = new Path(conf.get(CHR_INPUT_PROP));
        FSDataInputStream ins = chrOrderPath.getFileSystem(conf).open(chrOrderPath);
        AsciiLineReaderIterator it2 = new AsciiLineReaderIterator(new AsciiLineReader(ins));
        String line;  
        int i = 0;
        while(it2.hasNext()){
        	line = it2.next();
        	//System.err.println("fai:" + line);
        	String[] cols = line.split("\t");
        	chrOrder.put(cols[0].trim(), i++);
        }
        it2.close();
	}
	
	private boolean foundHeader(Object h){
		return (h instanceof FeatureCodecHeader) && (((FeatureCodecHeader)h).getHeaderValue() instanceof VCFHeader);
	}
	
	private boolean keepReading(AsciiLineReaderIterator it, long currentPos){
		return it.hasNext() && it.getPosition() <= currentPos && it.peek().startsWith("##");
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
	public Text getCurrentValue()  { return value;}
	
	@Override 
	public boolean nextKeyValue() throws IOException {
		if( !it.hasNext() || it.getPosition() > length) {
			return false;
		}
		final String line = it.next();
		String[] lineSplits = line.split("\t");
		String chr = lineSplits[0];
		long chrID = (long) chrOrder.get(chr);
		long pos = Long.parseLong(lineSplits[1]);
		key.set((chrID << 32) + pos);
		value.set(new Text(line)); 
	  
		return true;
	}
}