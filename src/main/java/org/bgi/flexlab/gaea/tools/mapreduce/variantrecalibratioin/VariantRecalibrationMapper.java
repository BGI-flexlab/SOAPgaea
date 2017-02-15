package org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.tools.mapreduce.vcfqualitycontrol.VCFQualityControlOptions;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.DBResource;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.FileResource;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.TrainData;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.ResourceManager;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.samtools.reference.FastaSequenceFile;
import htsjdk.variant.variantcontext.VariantContext;

public class VariantRecalibrationMapper extends Mapper<LongWritable, VariantContextWritable, IntWritable, Text>{

	private VCFQualityControlOptions options;
	
	private ResourceManager manager;
	
	private GenomeLocationParser genomeLocParser;
	
	/**
	 * 任务初始化设置
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		options = new VCFQualityControlOptions();
		options.getOptionsFromHadoopConf(context.getConfiguration());
		
		FastaSequenceFile ref = new FastaSequenceFile(new File(options.getReference()), true);
		genomeLocParser = new GenomeLocationParser(ref.getSequenceDictionary());
		ref.close();

		manager = new ResourceManager(options);
		for(String resource : options.getResources()) {
			TrainData trainData = new TrainData(options.getReference(), resource);
			if(trainData.isDB()) {
				trainData.setType(new DBResource());
			} else {
				trainData.setType(new FileResource());
			}
			trainData.initialize();
			manager.addTrainingSet(trainData);
		}
		if( !manager.checkHasTrainingSet() ) {
			throw new UserException.CommandLineException( "No training set found! Please provide sets of known polymorphic loci marked with the training=true ROD binding tag. For example, -resource:hg19-hapmap" );
		}
		if( !manager.checkHasTruthSet() ) {
			throw new UserException.CommandLineException( "No truth set found! Please provide sets of known polymorphic loci marked with the truth=true ROD binding tag. For example, -resource:hg19-hapmap" );
		}
	}
	
	@Override
	public void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
		VariantContext vc = value.get();
		if(!validContext(vc))
			return;
		
		VariantDatumMessenger datum = new VariantDatumMessenger.Builder(manager, vc, options)
														 .decodeAnnotations()
														 .setLoc(genomeLocParser)
														 .setOriginalQual()
														 .setFlagV()
														 .setPrior()
														 .build();
		if(datum != null) {
			context.write(new IntWritable((int)key.get()), new Text(datum.toString()));
		}
	}

	public boolean validContext(VariantContext vc) {
		return vc != null && 
				(vc.isNotFiltered() || options.getIgnoreInputFilters().containsAll(vc.getFilters())) &&
				ResourceManager.checkVariationClass(vc, options.getMode());
	}
	
}
