package org.bgi.flexlab.gaea.data.structure.pileup2;

public class PileupInformation {
	private byte information;
	private boolean snp;
	private boolean insertion;
	private boolean deletion;
	
	public PileupInformation(){
		information =  0;
		snp = false;
		insertion = false;
		deletion = false;
	}
	
	public PileupInformation(boolean isInsertion,boolean isDeletion){
		this.insertion = isInsertion;
		this.deletion = isDeletion;
	}
	
	public PileupInformation(byte base,byte quality){
		snp = true;
		information = (byte)(((base & 0x3)<<6) | (quality & 0x3f));
	}
	
	public byte getBase(){
		if(!snp){
			if(deletion){
				return -1;
			}
			else if(insertion){
				return -2;
			}
			return -3;
		}
		return (byte)((information >> 6) & 0x3);
	}
	
	public byte getQuality(){
		if(!snp){
			if(deletion){
				return -1;
			}
			else if(insertion){
				return -2;
			}
			return -3;
		}
		return (byte)(information & 0x3f);
	}
	
	public void add(boolean isInsertion,boolean isDeletion){
		if(!snp){
			this.insertion = isInsertion;
			this.deletion = isDeletion;
		}
	}
	
	public void add(byte base,byte quality){
		snp = true;
		byte qual = getQuality();
		if(qual < quality){
			information = (byte)(((base & 0x3)<<6) | (quality & 0x3f));
		}
	}
	
	public boolean isSNP(){
		return snp;
	}
	
	public boolean isInsertion(){
		return insertion;
	}
	
	public boolean isDeletion(){
		return deletion;
	}
}

