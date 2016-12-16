package org.bgi.flexlab.gaea.data.structure.bam.clipper.algorithm;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.clipper.ClippingRegion;

public class ClippingWriteNs implements ReadClippingAlgorithm{

	@Override
	public GaeaSamRecord apply(GaeaSamRecord read, ClippingRegion cr) {
		byte[] bases = read.getReadBases();
		
		for(int i = cr.getStart() ; i < cr.getStop() ; i++){
			bases[i] = 'N';
		}
		
		read.setReadBases(bases);
		
		return read;
	}
}
