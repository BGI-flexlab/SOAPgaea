package org.bgi.flexlab.gaea.data.structure.reference;

import org.bgi.flexlab.gaea.exception.OutOfBoundException;

public class BaseAndSNPInformation {
	private byte[] bytes = null;
	private int start;
	
	public BaseAndSNPInformation(byte[] bytes,int start){
		this.bytes = bytes;
		this.start = start;
	}
	
	public void setBytes(byte[] bytes){
		this.bytes = bytes;
	}
	
	public void setStart(int start){
		this.start = start;
	}
	
	public boolean[] getSNPs(){
		if(bytes == null)
			return null;
		int len = bytes.length;
		boolean[] snps = new boolean[len];
		
		for(int i = 0 ; i < len ; i++){
			if(((bytes[i] >> 3) & 0x1) == 0)
				snps[i] = false;
			else
				snps[i] = true;
		}
		
		return snps;
	}
	
	public boolean getSNP(int pos){
		int index = pos - start;
		if(index >= bytes.length)
			throw new OutOfBoundException(pos,start+bytes.length);
		
		if(((bytes[index] >> 3 ) & 0x1) == 0)
			return false;
		return true;
	}
	
	public byte[] getBytes(){
		return bytes;
	}
}
