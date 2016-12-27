package org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.VCFRecalibrator;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.samtools.reference.FastaSequenceFile;
import htsjdk.variant.variantcontext.VariantContext;


public class VariantRecalibrationReducer extends Reducer<IntWritable, Text, NullWritable, VariantContextWritable>{
	private VCFRecalibrator recal;
	private VariantRecalibrationOptions options;
	private List<VariantContext> vcs = new ArrayList<>();
	private int fileId;
	private GenomeLocationParser genomeLocParser;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
        options = new VariantRecalibrationOptions();
        options.getOptionsFromHadoopConf(conf);
		recal = new VCFRecalibrator(options, conf);
		FastaSequenceFile ref = new FastaSequenceFile(new File(options.getReference()), true);
		genomeLocParser = new GenomeLocationParser(ref.getSequenceDictionary());
		ref.close();
    }
	
    @Override
	public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException {
    	fileId = key.get();
    	for(Text value : values) {
	    	VariantDatumMessenger msg = new VariantDatumMessenger.Builder().
	    							buildFrom(value.toString(), genomeLocParser);
	    	recal.addData(msg);
    	}
    	recal.recalVCF(fileId, context);
    }
    
    @Override
    public void cleanup(Context context) {
    }
}
