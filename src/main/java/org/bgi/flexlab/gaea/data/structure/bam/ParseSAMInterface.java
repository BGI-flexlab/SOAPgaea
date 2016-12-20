package org.bgi.flexlab.gaea.data.structure.bam;

public interface ParseSAMInterface {
	public boolean parseSAM(String samline);

	public boolean parseSAM(GaeaSamRecord samRecord);
	
	public boolean SAMFilter();
}
