package org.bgi.flexlab.gaea.tools.annotator.interval;

import org.bgi.flexlab.gaea.tools.annotator.conf.Config;
import org.bgi.flexlab.gaea.tools.annotator.interval.tree.IntervalForest;
import org.bgi.flexlab.gaea.tools.annotator.interval.tree.Itree;
import org.bgi.flexlab.gaea.tools.annotator.util.Gpr;
import org.bgi.flexlab.gaea.tools.annotator.util.Timer;

/**
 * Cytband definitions
 * E.g.: http://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/cytoBand.txt.gz
 *
 * @author pcingola
 */
public class CytoBands {

	public static final String DEFAULT_CYTOBAND_BED_FILE = "cytoBand.txt.gz";

	boolean verbose;
	boolean debug;
	Genome genome;
	IntervalForest forest;

	public CytoBands(Genome genome) {
		Config config = Config.get();
		this.genome = genome;
		verbose = config.isVerbose();
		debug = config.isDebug();
		forest = new IntervalForest();

		String cytoBandFile = config.getDirDataGenomeVersion() + "/" + DEFAULT_CYTOBAND_BED_FILE;
		if (Gpr.exists(cytoBandFile)) load(cytoBandFile);
		else if (debug) Gpr.debug("Cannot open file '" + cytoBandFile + "', not loadng cytobands");
	}

	public void add(Marker m) {
		forest.add(m);
	}

	public void build() {
		forest.build();
	}

	public boolean isEmpty() {
		return forest.size() <= 0;
	}

	/**
	 * Load cytobands form BED interval
	 */
	void load(String bedFile) {
		if (verbose) Timer.showStdErr("Loading cytobands form file '" + bedFile + "'");
		BedFileIterator bed = new BedFileIterator(bedFile);

		int count = 0;
		for (Variant var : bed) {
			add(var);
			count++;
		}

		if (verbose) {
			if (count <= 0) Timer.showStdErr("WARNING: Unable to load cytobands from file '" + bedFile + "'");
			else Timer.showStdErr("Loaded " + count + " cytoband intervals");
		}

		build();
	}

	public Markers query(Marker marker) {
		return forest.query(marker);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Cytobands for " + genome.getId() + " : " + (isEmpty() ? "[Empty]" : "") + "\n");

		for (Itree tree : forest)
			for (Marker m : tree)
				sb.append("\t" + m.getChromosomeName() + "\t" + m.getStart() + "\t" + m.getEnd() + "\t" + m.getId() + "\n");

		return sb.toString();
	}

}
