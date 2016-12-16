package org.bgi.flexlab.gaea.data.structure.bam.clipper.algorithm;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.clipper.ClippingRegion;

public class ClippingWriteZeros implements ReadClippingAlgorithm{

	@Override
	public GaeaSamRecord apply(GaeaSamRecord read, ClippingRegion cr) {
		byte[] quals = read.getBaseQualities();
		
		for(int i = cr.getStart() ; i < cr.getStop() ; i++){
			quals[i] = 0;
		}
		
		read.setBaseQualities(quals);
		
		return read;
	}

}
