package org.bgi.flexlab.gaea.data.structure.bam.clipper.algorithm;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.clipper.ClippingRegion;

public class ClippingWriteNsAndQualityZero implements ReadClippingAlgorithm{
	
	@Override
	public GaeaSamRecord apply(GaeaSamRecord read, ClippingRegion cr) {
		byte[] bases = read.getReadBases();
		byte[] quals = read.getBaseQualities();
		
		for(int i = cr.getStart() ; i < cr.getStop() ; i++){
			bases[i] = 'N';
			quals[i] = 0;
		}
		
		read.setReadBases(bases);
		read.setBaseQualities(quals);
		
		return read;
	}
}
