package org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

import htsjdk.variant.variantcontext.VariantContext;

public interface ResourceType {

	public void initialize(String reference, String dbSnp);
	
	public ArrayList<VariantContext> get(GenomeLocation loc);
}
