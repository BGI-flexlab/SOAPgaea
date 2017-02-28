package org.bgi.flexlab.gaea.data.structure.bam;

public interface ParseSAMInterface {
	public boolean parseBamQC(String samline);

	public boolean parseSam(String samRecord);
	
	public boolean SAMFilter();
}
