package org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.VCFRecalibrator;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.bgi.flexlab.gaea.tools.vcf.report.ReportDatum;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.samtools.reference.FastaSequenceFile;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;


public class VariantRecalibrationReducer extends Reducer<IntWritable, Text, NullWritable, VariantContextWritable>{
	private VCFRecalibrator recal;
	private VariantRecalibrationOptions options;
	private int fileId;
	private GenomeLocationParser genomeLocParser;
    private MultipleVCFHeader headers;
    private ReportDatum report;
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
        options = new VariantRecalibrationOptions();
        options.getOptionsFromHadoopConf(conf);
		recal = new VCFRecalibrator(options, conf);
		FastaSequenceFile ref = new FastaSequenceFile(new File(options.getReference()), true);
		genomeLocParser = new GenomeLocationParser(ref.getSequenceDictionary());
		ref.close();
		headers = (MultipleVCFHeader) GaeaVCFHeader.loadVcfHeader(false, conf);
    }
	
    @Override
	public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	fileId = key.get();
    	for(Text value : values) {
	    	VariantDatumMessenger msg = new VariantDatumMessenger.Builder().
	    							buildFrom(value.toString(), genomeLocParser);
	    	recal.addData(msg);
    	}
    	recal.recalVCF(fileId, context);
    	
    	VCFHeader header = headers.getVcfHeader(fileId);
    	header = recal.addHeaderLine(header);
    	VCFCodec codec = new VCFCodec();
		codec.setVCFHeader(header, VCFHeaderVersion.VCF4_2);
    	InputStream is = HdfsFileManager.getInputStream(new Path(headers.getFile(fileId)), context.getConfiguration());
		AsciiLineReaderIterator iterator = new AsciiLineReaderIterator(new AsciiLineReader(is));
		while(iterator.hasNext()) {
			VariantContext vc = codec.decode(iterator.next());
			if(vc == null)
				continue;
			vc = recal.applyRecalibration(vc);
			statistic(vc);
			VariantContextWritable vcWritable = new VariantContextWritable();
			vcWritable.set(vc);
			context.write(NullWritable.get(), vcWritable);
		}
		iterator.close();
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
    	FSDataOutputStream os = HdfsFileManager.getOutputStream(new Path(options.getReportPath()), context.getConfiguration());
		os.write(report.formatReport().getBytes());
		os.close();
    }
    
    public void statistic(VariantContext vc) {
    	ReportDatum datum = new ReportDatum.Builder(vc).isSnp().isIndel().isTransition().build();
		if(report == null)
			report = datum;
		else
			report.combine(datum);
    }
}
