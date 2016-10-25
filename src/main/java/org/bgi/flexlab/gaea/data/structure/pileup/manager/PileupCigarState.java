package org.bgi.flexlab.gaea.data.structure.pileup.manager;

import htsjdk.samtools.CigarOperator;

public class PileupCigarState {
	private boolean deletion;
	private boolean beforeDeletion;
	private boolean afterDeletion;
	private boolean beforeInsertion;
	private boolean afterInsertion;
	private boolean nextToSoftClip;

	public PileupCigarState(CigarOperator nextOp, CigarOperator lastOp,
			boolean isSingle) {
		beforeDeletion = nextOp == CigarOperator.DELETION;
		afterDeletion = lastOp == CigarOperator.DELETION;
		beforeInsertion = nextOp == CigarOperator.INSERTION;
		afterInsertion = lastOp == CigarOperator.INSERTION && !isSingle;
	}
	
	public void setDeletion(boolean deletion){
		this.deletion = deletion;
	}
	
	public void setNextToSoftClip(boolean nextToSoft){
		this.nextToSoftClip = nextToSoft;
	}
	
	public boolean isDeletion(){
		return this.deletion;
	}
	
	public boolean isBeforeDeletion(){
		return this.beforeDeletion;
	}
	
	public boolean isAfterDeletion(){
		return this.afterDeletion;
	}
	
	public boolean isBeforeInsertion(){
		return this.beforeInsertion;
	}
	
	public boolean isAfterInsertion(){
		return this.afterInsertion;
	}
	
	public boolean isNextToSoftClip(){
		return this.nextToSoftClip;
	}
}
