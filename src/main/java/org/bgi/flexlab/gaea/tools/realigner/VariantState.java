package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;

public class VariantState {
	private boolean hasIndel;
	private boolean hasInsertion;
	private boolean hasSNP;
	private int furthestPos;
	
	public VariantState(){
		hasIndel = hasInsertion = hasSNP = false;
		furthestPos = -1;
	}
	
	public boolean filterVariant(ArrayList<VariantContext> knowIndels,long position){
		if(knowIndels == null)
			return false;
		
		for(VariantContext indel : knowIndels){
			if(indel.getStart() <= position && indel.getEnd() >= position){
				switch(indel.getType()){
				case MIXED:
					hasSNP = true;
					hasIndel = true;
					if(indel.isSimpleInsertion())
						hasInsertion = true;
					break;
				case INDEL:
					hasIndel = true;
					if(indel.isSimpleInsertion())
						hasInsertion = true;
					break;
				case SNP:
					hasSNP = true;
					break;
				default:
					break;
				}
				
				if(hasIndel)
					furthestPos = indel.getEnd();
			}
		}
		
		return true;
	}
	
	public boolean isInsertion(){
		return this.hasInsertion;
	}
	
	public boolean isIndel(){
		return this.hasIndel;
	}
	
	public boolean isSNP(){
		return this.hasSNP;
	}
	
	public int getFurthestPosition(){
		return this.furthestPos;
	}
}
