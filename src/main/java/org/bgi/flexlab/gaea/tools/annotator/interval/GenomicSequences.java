package org.bgi.flexlab.gaea.tools.annotator.interval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.interval.Chromosome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Exon;
import org.bgi.flexlab.gaea.tools.annotator.interval.Gene;
import org.bgi.flexlab.gaea.tools.annotator.interval.Genome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Marker;
import org.bgi.flexlab.gaea.tools.annotator.interval.MarkerSeq;
import org.bgi.flexlab.gaea.tools.annotator.interval.Markers;
import org.bgi.flexlab.gaea.tools.annotator.interval.Transcript;
import org.bgi.flexlab.gaea.tools.annotator.interval.tree.IntervalForest;
import org.bgi.flexlab.gaea.tools.annotator.interval.tree.Itree;
import org.bgi.flexlab.gaea.tools.annotator.util.Gpr;
import org.bgi.flexlab.gaea.tools.annotator.util.GprSeq;
import org.bgi.flexlab.gaea.tools.annotator.util.Timer;

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

	public GenomicSequences(Genome genome) {
		this.genome = genome;
	}

}
