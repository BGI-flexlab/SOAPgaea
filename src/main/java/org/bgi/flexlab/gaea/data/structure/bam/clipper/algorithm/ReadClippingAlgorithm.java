package org.bgi.flexlab.gaea.data.structure.bam.clipper.algorithm;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.clipper.ClippingRegion;

public interface ReadClippingAlgorithm {
	public GaeaSamRecord apply(GaeaSamRecord read,ClippingRegion cr);
}
