package org.bgi.flexlab.gaea.tools.jointcalling.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

import htsjdk.tribble.Feature;
import htsjdk.variant.variantcontext.VariantContext;

public class RefMetaDataTracker {
	private GenomeLocation location = null;
	
	public RefMetaDataTracker(GenomeLocation location){
		this.location = location;
	}
	
	public GenomeLocation getLocation(){
		return location;
	}
	
	public static List<VariantContext> getValues(ArrayList<VariantContext> binding,GenomeLocation location){
		if(location.getStart() != location.getStop())
			throw new UserException("location must length is 1!");
		List<VariantContext> results = new ArrayList<VariantContext>();
		int pos = location.getStart();
		for(VariantContext context : binding){
			if(context.getStart() >= pos && context.getEnd() <= pos)
				results.add(context);
		}
		
		return results;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Feature> List<T> getValues(RodBinding<VariantContext> binding,GenomeLocation loc){
		return Collections.EMPTY_LIST;
	}
}
