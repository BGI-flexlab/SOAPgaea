package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

import htsjdk.variant.variantcontext.VariantContext;

public class TrainData {	
	
	private String contextName;
	
	private ResourceType ram;
	
	private String ref;
	/**
	 * default constructor
	 */
	public TrainData(String ref, String resource) {
		this.ref = ref;
		ResourceTag.parseTag(resource);
		contextName = (isDB() ? ResourceTag.valueOf("DB").getProperty(): ResourceTag.valueOf("FILE").getProperty());
	}

	public void setType(ResourceType ram) {
		this.ram = ram;
	}
	
	public void initialize() {
		ram.initialize(ref, contextName);
	}
	
	public ArrayList<VariantContext> get(GenomeLocation loc) {
		// TODO Auto-generated method stub
		return ram.get(loc);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(ResourceTag tag : ResourceTag.values()) {
			sb.append(tag.getProperty());
			sb.append("\t");
		}
		return sb.toString();
	}
	
	public Object clone() {  
		TrainData o = null;  
        try {  
            o = (TrainData) super.clone();  
        } catch (CloneNotSupportedException e) {  
            e.printStackTrace();  
        }  
        return o; 
    }  
	
	/**
	 * @return the name
	 */
	public String getName() {
		return ResourceTag.valueOf("NAME").getProperty();
	}

	/**
	 * @return the isKnown
	 */
	public boolean isKnown() {
		return Boolean.parseBoolean(ResourceTag.valueOf("KNOWN").getProperty());
	}

	/**
	 * @return the isTraining
	 */
	public boolean isTraining() {
		return Boolean.parseBoolean(ResourceTag.valueOf("TRAINING").getProperty());
	}

	/**
	 * @return the isAntiTraining
	 */
	public boolean isAntiTraining() {
		return Boolean.parseBoolean(ResourceTag.valueOf("ANTITRAINING").getProperty());
	}

	public boolean isDB() {
		return ResourceTag.valueOf("DB").getProperty() != null;
	}
	
	/**
	 * @return the isTruth
	 */
	public boolean isTruth() {
		return Boolean.parseBoolean(ResourceTag.valueOf("TRUTH").getProperty());
	}

	/**
	 * @return the isConsensus
	 */
	public boolean isConsensus() {
		return Boolean.parseBoolean(ResourceTag.valueOf("CONSENSUS").getProperty());
	}

	/**
	 * @return the prior
	 */
	public double getPrior() {
		return Double.parseDouble(ResourceTag.valueOf("PRIOR").getProperty());
	}

	public String getContextName() {
		return contextName;
	}
	
	public static void main(String[] args) {
		TrainData td = new TrainData("ref", "name=hapmap,training=true,truth=true,prior=15.0,file=hapmap_3.3.b37.sites.vcf");
		DBResource db = new DBResource();
		td.setType(db);
		td.initialize();
		System.out.println(td.isKnown());
	}
}
