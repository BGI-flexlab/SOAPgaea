package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.xerces.impl.dv.ValidationContext;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibrationOptions;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.model.VariantDataManager;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.QualityUtils;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextUtils;

public class VariantDatumMessenger{

	private double[] annotations;
	private boolean[] isNull;
	private double lod;
	
	/**
	 * isKnown:atTruthSite:atTrainingSite:atAntiTrainingSite:isTransition:isSNP:failingSTDThreshold
	 */
	public static final byte isKnown = 0x1;
	public static final byte atTruthSite = 0x1 << 1;
	public static final byte atTrainingSite = 0x1 << 2;
	public static final byte atAntiTrainingSite = 0x1 << 3;
	public static final byte isTransition = 0x1 << 4;
	public static final byte isSNP = 0x1 << 5;
	public static final byte failingSTDThreshold = 0x1 << 6;
	private byte flag;

	private double originalQual;
	private double prior;
	private int consensusCount;
	private GenomeLocation loc;
	private int worstAnnotation;
	
	private VariantDatumMessenger(Builder builder) {
		this.annotations = builder.annotations;
		this.isNull = builder.isNull;
		this.lod = builder.lod;
		this.flag = builder.flag;
		this.originalQual = builder.originalQual;
		this.prior = builder.prior;
		this.consensusCount = builder.consensusCount;
		this.loc = builder.loc;
		this.worstAnnotation = builder.worstAnnotation;
	}
	
	public String toString() {
		StringBuilder datumInfo = new StringBuilder();
		datumInfo.append(annotations.length);
		for(int i = 0; i < isNull.length; i++) {
			datumInfo.append("\t");
			datumInfo.append(isNull[i]);
		}
		
		for(int i = 0; i < annotations.length; i++) {
			datumInfo.append("\t");
			datumInfo.append(annotations[i]);
		}
		
		datumInfo.append("\t");datumInfo.append(lod);
		datumInfo.append("\t");datumInfo.append(flag);
		datumInfo.append("\t");datumInfo.append(originalQual);
		datumInfo.append("\t");datumInfo.append(prior);
		datumInfo.append("\t");datumInfo.append(consensusCount);
		datumInfo.append("\t");datumInfo.append(loc.getContig());
		datumInfo.append("\t");datumInfo.append(loc.getStart());
		datumInfo.append("\t");datumInfo.append(loc.getStop());
		datumInfo.append("\t");datumInfo.append(worstAnnotation);
		
		return datumInfo.toString();
	}
	
	public void parseHadoopLine(String line, GenomeLocationParser genomeLocParser) {
		String[] lineSplits = line.split("\t");
		
		int index = 0;
		//int id = Integer.parseInt(lineSplits[index++]);
		int annoSize = Integer.parseInt(lineSplits[index++]);
		isNull = new boolean[annoSize];
		for(int i = 0; i < annoSize; i++) {
			isNull[i] = Boolean.parseBoolean(lineSplits[index++]);
		}
		
		annotations = new double[annoSize];
		for(int i = 0; i < annoSize; i++) {
			if(!isNull[i])
				annotations[i] = Double.parseDouble(lineSplits[index++]);
			else
				index++;
		}
		
		lod = Double.parseDouble(lineSplits[index++]);
		flag = Byte.parseByte(lineSplits[index++]);
		originalQual = Double.parseDouble(lineSplits[index++]);
		prior = Double.parseDouble(lineSplits[index++]);
		consensusCount = Integer.parseInt(lineSplits[index++]);
		loc = genomeLocParser.createGenomeLocation(lineSplits[index++], Integer.parseInt(lineSplits[index++]), Integer.parseInt(lineSplits[index++]));
		worstAnnotation = Integer.parseInt(lineSplits[index++]);
		//return id;
	}
	
	public double[] getAnnotations() {
		return annotations;
	}
	
	public boolean[] getIsNull() {
		return isNull;
	}
	
	public double getLod() {
		return lod;
	}
	
	public byte getFlag() {
		return flag;
	}
	
	public boolean checkFlag(byte i) {
		return (flag & i) != 0;
	}
	  
	public static boolean checkFlag(byte flag, byte i) {
		return (flag & i) != 0;
	}
	public double getOriginalQual() {
		return originalQual;
	}
	
	public double getPrior() {
		return prior;
	}
	
	public int getConsensusCount() {
		return consensusCount;
	}
	
	public GenomeLocation getLoc() {
		return loc;
	}
	
	public int getWorstAnnotation() {
		return worstAnnotation;
	}
	
	public static class Builder {
		
		private double[] annotations;
		private boolean[] isNull;
		private double lod;
	
		private byte flag;

		private double originalQual;
		private double prior;
		private int consensusCount;
		private GenomeLocation loc;
		private int worstAnnotation;
		
		private VariantRecalibrationOptions options;
		
		private VariantDataManager manager;
				
		private VariantContext vc;
				
		public Builder(VariantDataManager manager, VariantContext vc, VariantRecalibrationOptions options) {
		// TODO Auto-generated constructor stub
			this.annotations = null;
			this.isNull = null;
			this.lod = 0.0;
			this.flag = 0;
			this.originalQual = 0;
			this.prior = 2.0;
			this.consensusCount = 0;
			this.loc = null;
			this.worstAnnotation = 0;
			this.options = options;
			this.vc = vc;
			this.manager = manager;
		}
		
		public Builder setAnnotations() {
			manager.decodeAnnotations(vc, true);
			this.annotations = manager.getAnnotations();
			this.isNull = manager.getIsNull();
			return this;
		}
		
		public Builder setFlagV() throws IOException {
			if(vc.isSNP() && vc.isBiallelic())
				this.flag = (this.flag |= VariantDatumMessenger.isSNP);
			if(checkFlag(this.flag, VariantDatumMessenger.isSNP) && VariantContextUtils.isTransition(vc))
				this.flag = (this.flag |= VariantDatumMessenger.isTransition);

			flag = parseTrainingSets(flag, options.isTrustAllPolymorphic());
			return this;
		}
		
		public Builder setOriginalQual() {
			this.originalQual = vc.getPhredScaledQual();
			return this;
		}
		
		public Builder setPrior() {
			double priorFactor = QualityUtils.qualToProb(this.prior);
			this.prior = Math.log10( priorFactor ) - Math.log10( 1.0 - priorFactor );
			return this;
		}
		
		public Builder setLoc(GenomeLocationParser genomeLocParser) {
			this.loc = genomeLocParser.createGenomeLocation(vc);
			return this;
		}
		
	    private byte parseTrainingSets(byte flag, final boolean TRUST_ALL_POLYMORPHIC ) throws IOException {
	        byte result = flag;
	    	for( final TrainData trainingData : manager.getTrainDataSet() ) {
	            for( final VariantContext trainVC : trainingData.get(loc) ) {
	            	if( isValidVariant( vc, trainVC, TRUST_ALL_POLYMORPHIC ) ) {
	                    if(checkFlag(this.flag, VariantDatumMessenger.isKnown) || trainingData.isKnown()) {
	                    	result = (result |= VariantDatumMessenger.isKnown);
	                    }
	                    if(checkFlag(this.flag, VariantDatumMessenger.atTruthSite) || trainingData.isTruth()) {
	                    	result = (result |= VariantDatumMessenger.atTruthSite);
	                    }
	                    if(checkFlag(this.flag, VariantDatumMessenger.atTrainingSite) || trainingData.isTraining()) {
	                    	result = (result |= VariantDatumMessenger.atAntiTrainingSite);
	                    }
	                    this.prior = Math.max(this.prior, trainingData.getPrior());
	                }
	                if( trainVC != null ) {
	                	if(checkFlag(this.flag, VariantDatumMessenger.atAntiTrainingSite) || trainingData.isAntiTraining()) {
	                    	result = (result |= VariantDatumMessenger.atAntiTrainingSite);
	                    }
	                }
	            }
	        }
	    	return result;
	    }
	    
	    private boolean isValidVariant( final VariantContext evalVC, final VariantContext trainVC, final boolean TRUST_ALL_POLYMORPHIC) {
	        return trainVC != null && trainVC.isNotFiltered() && trainVC.isVariant() && VariantDataManager.checkVariationClass( evalVC, trainVC ) &&
	                        (TRUST_ALL_POLYMORPHIC || !trainVC.hasGenotypes() || trainVC.isPolymorphicInSamples());
	    }
	    
		public VariantDatumMessenger build() {
			return new VariantDatumMessenger(this);
		}
	}
	
}
