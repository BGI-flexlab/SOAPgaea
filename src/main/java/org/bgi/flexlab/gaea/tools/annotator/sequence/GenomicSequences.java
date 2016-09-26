package org.bgi.flexlab.gaea.tools.annotator.sequence;

import java.io.Serializable;

import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.annotator.interval.Genome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Marker;
import org.bgi.flexlab.gaea.tools.annotator.util.GprSeq;

/**
 * This class stores all "relevant" sequences in a genome
 *
 * This class is able to:
 * 		i) Add all regions of interest
 * 		ii) Store genomic sequences for those regions of interest
 * 		iii) Retrieve genomic sequences by interval
 *
 *
 * @author pcingola
 */
public class GenomicSequences implements Serializable {

	private static final long serialVersionUID = 2339867422366567569L;
	public static final int MAX_ITERATIONS = 1000000;
	public static final int CHR_LEN_SEPARATE_FILE = 1000 * 1000; // Minimum chromosome length to be saved to a separate file

	boolean debug = false;
	boolean verbose = false;
	boolean allSmallLoaded; // Have all "small" chromosomes been loaded? (i.e. have we already loaded 'sequence.bin' file?)
	boolean disableLoad = false; // Do not load sequences from disk. Used minly for test cases
	Genome genome; // Reference genome
	private ChromosomeInformationShare chrShare=null;

	public GenomicSequences(Genome genome) {
		this.genome = genome;
	}
	
	
	/**
	 * Get sequence for a marker
	 */
	public String querySequence(Marker marker) {
		marker.getChromosomeName();
		// Calculate start and end coordiantes
		String seq = chrShare.getBaseSequence(marker.getStart(), marker.getEnd());
		// Return sequence in same direction as 'marker'
		if (marker.isStrandMinus()) seq = GprSeq.reverseWc(seq);
		return seq;
	}

}
