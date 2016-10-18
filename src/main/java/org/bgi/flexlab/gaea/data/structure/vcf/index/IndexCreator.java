package org.bgi.flexlab.gaea.data.structure.vcf.index;
import java.io.IOException;


public interface IndexCreator {
	
	public void initialize(String inputFile, int binSize);
	
	@SuppressWarnings("rawtypes")
	public Index finalizeIndex() throws IOException;
	
	public int defaultBinSize();
	
	public int getBinSize();
}
