package org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.model.VariantDataManager;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.DBResource;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.FileResource;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.TrainData;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.samtools.reference.FastaSequenceFile;
import htsjdk.variant.variantcontext.VariantContext;

public class VariantRecalibrationMapper extends Mapper<LongWritable, VariantContextWritable, IntWritable, Text>{

	private VariantRecalibrationOptions options;
	
	private VariantDataManager manager;
	
	private GenomeLocationParser genomeLocParser;
	
	/**
	 * 任务初始化设置
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		options = new VariantRecalibrationOptions();
		options.getOptionsFromHadoopConf(context.getConfiguration());
		
		FastaSequenceFile ref = new FastaSequenceFile(new File(options.getReference()), true);
		genomeLocParser = new GenomeLocationParser(ref.getSequenceDictionary());
		ref.close();

		manager = new VariantDataManager( options.getUseAnnotations(), options );
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
														 .setAnnotations()
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
				VariantDataManager.checkVariationClass(vc, options.getMode());
	}
}
