package org.bgi.flexlab.gaea.tools.mapreduce.jointcalling;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.tools.jointcalling.UnifiedGenotypingEngine.GenotypingOutputMode;
import org.bgi.flexlab.gaea.tools.jointcalling.UnifiedGenotypingEngine.OutputMode;

public class JointCallingOptions extends GaeaOptions implements HadoopOptions{
	private int samplePloidy = 2;
	
	private int MAX_ALTERNATE_ALLELES = 6;
	
	private int MAX_NUM_PL_VALUES = 100;
	
	private String output = null;
	
	private List<String> input = Collections.emptyList();
	
	private double snpHeterozygosity = 1e-3;
	
	private double indelHeterozygosity = 1.0/8000;
	
	private List<Double> inputPrior = Collections.emptyList();
	
	private OutputMode outputMode = OutputMode.EMIT_VARIANTS_ONLY;
	
	private GenotypingOutputMode genotypeMode = GenotypingOutputMode.DISCOVERY;
	
	public boolean ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED = false;
	
	public boolean ANNOTATE_ALL_SITES_WITH_PL = false;
	
	public double STANDARD_CONFIDENCE_FOR_CALLING = 30.0;
	
	public double STANDARD_CONFIDENCE_FOR_EMITTING = 30.0;

	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		conf.setStrings("args", args);
	}

	@Override
	public void getOptionsFromHadoopConf(Configuration conf) {
		String[] args = conf.getStrings("args");
		this.parse(args);
	}

	@Override
	public void parse(String[] args) {
		
	}

	public int getSamplePloidy(){
		return this.samplePloidy;
	}
	
	public double getSNPHeterozygosity(){
		return this.snpHeterozygosity;
	}
	
	public double getINDELHeterozygosity(){
		return this.indelHeterozygosity;
	}
	
	public List<Double> getInputPrior(){
		return this.inputPrior;
	}
	
	public OutputMode getOutputMode(){
		return this.outputMode;
	}
	
	public GenotypingOutputMode getGenotypingOutputMode(){
		return this.genotypeMode;
	}
	
	public int getMaxAlternateAllele (){
		return this.MAX_ALTERNATE_ALLELES;
	}
	
	public int getMaxNumberPLValues(){
		return this.MAX_NUM_PL_VALUES;
	}
	
	public String getOutput(){
		return this.output;
	}
	
	public List<String> getInput(){
		return this.input;
	}
}
