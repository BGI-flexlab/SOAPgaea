package org.bgi.flexlab.gaea.data.structure.dbsnp;

import java.util.Arrays;

import org.bgi.flexlab.gaea.data.structure.memoryshare.BioMemoryShare;

import htsjdk.variant.variantcontext.VariantContext.Type;

public class ChromosomeDbsnpShare extends BioMemoryShare {

	public ChromosomeDbsnpShare() {
		super(Byte.SIZE / 2);
	}

	/**
	 * start and end are minimum : 0 ,maximum :length-1
	 */
	public Type[] getSnpInformation(int start, int end) {
		byte[] snps = getBytes(start, end);
		int len = snps.length;
		int i;
		
		byte[] bytes = new byte[len*4];
		for(i = 0 ; i < len ; i++){
			bytes[i<<2] = (byte)((snps[i]>>6) & 0x3);
			bytes[(i<<2) + 1] = (byte)((snps[i]>>4) & 0x3);
			bytes[(i<<2) + 2]= (byte)((snps[i]>>2) & 0x3);
			bytes[(i<<2) + 3] = (byte)((snps[i]) & 0x3);
		}
		
		Type[] types = new Type[end-start+1];
		Arrays.fill(types, Type.NO_VARIATION);
		
		int s = start - ((start>>2)<<2);
		int e = end - ((start>>2)<<2) + 1;
		
		for( i = s ; i < e ; i++){
			if(i >= len)
				break;
			
			if(bytes[i] == 1)
				types[i-s] = Type.SNP;
			else if(bytes[i] == 2)
				types[i-s] = Type.INDEL;
			else if(bytes[i] == 3)
				types[i-s] = Type.MIXED;
		}
		
		snps = null;

		return types;
	}

	public Type getSnpInformation(int position) {
		Type type = getSnpInformation(position, position)[0];
		return type;
	}
}
