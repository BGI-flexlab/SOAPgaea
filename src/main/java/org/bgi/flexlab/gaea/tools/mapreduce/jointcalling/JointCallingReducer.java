package org.bgi.flexlab.gaea.tools.mapreduce.jointcalling;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.tools.jointcalling.JointCallingEngine;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFHeader;

public class JointCallingReducer
		extends Reducer<WindowsBasedWritable, VariantContextWritable, NullWritable, VariantContextWritable> {

	private int windowSize = 10000;
	private HashMap<Integer,String> contigs = null;
	private JointCallingEngine engine = new JointCallingEngine();
	private VariantContextWritable outValue = new VariantContextWritable();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		VCFHeader header = null; 
		contigs = new HashMap<Integer,String>();
		
		List<VCFContigHeaderLine> lines = header.getContigLines();
		
		for(int i = 0 ; i < lines.size() ; i++){
			VCFContigHeaderLine line = lines.get(i);
			contigs.put(line.getContigIndex(), line.getID());
		}
	}

	@Override
	public void reduce(WindowsBasedWritable key, Iterable<VariantContextWritable> values, Context context)
			throws IOException, InterruptedException {
		int winNum = key.getWindowsNumber();
		int start = winNum * windowSize;
		int end = start + windowSize - 1;
		int chrIndex = key.getChromosomeIndex();
		
		engine.set(contigs.get(chrIndex), start, end);
		
		VariantContext variantContext = engine.variantCalling(values.iterator());
		outValue.set(variantContext);
		context.write(NullWritable.get(), outValue);
	}
}
