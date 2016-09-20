package org.bgi.flexlab.gaea.data.structure.pileup.filter;

import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;

public class PileupElementStrandFilter implements PileupElementFilter{
	private int strand ;
	
	public PileupElementStrandFilter(){
		this.strand = 0;
	}
	
	public PileupElementStrandFilter(int strand){
		this.strand = strand;
	}
	
	public void set(int strand){
		this.strand = strand;
	}

	@Override
	public boolean allow(PileupElement pileupElement) {
		if(strand == 0){
			return !pileupElement.getReadNegativeStrandFlag();
		}
		return pileupElement.getReadNegativeStrandFlag();
	}
}
