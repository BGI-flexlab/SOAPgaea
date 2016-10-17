package org.bgi.flexlab.gaea.data.structure.bam;

public interface ParseSAMInterface {
	public boolean parseSAM(String samline);
	
	public boolean SAMFilter();
}
