package org.bgi.flexlab.gaea.data.structure.location;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.util.Window;

public abstract class GenomeLocationFilter {
	public abstract boolean filter(GenomeLocation location,Window win);
	
	public ArrayList<GenomeLocation> filterList(ArrayList<GenomeLocation> intervals,Window win){
		ArrayList<GenomeLocation> filtered = new ArrayList<GenomeLocation>();
		
		for(GenomeLocation location : intervals){
			if(!filter(location,win)){
				filtered.add(location);
			}
		}
		
		return filtered;
	}
}
