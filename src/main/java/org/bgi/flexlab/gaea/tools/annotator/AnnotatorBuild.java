package org.bgi.flexlab.gaea.tools.annotator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo;
import org.bgi.flexlab.gaea.tools.annotator.effect.SnpEffectPredictor;
import org.bgi.flexlab.gaea.tools.annotator.effect.factory.SnpEffPredictorFactory;
import org.bgi.flexlab.gaea.tools.annotator.effect.factory.SnpEffPredictorFactoryRefSeq;
import org.bgi.flexlab.gaea.tools.annotator.interval.Gene;
import org.bgi.flexlab.gaea.tools.annotator.interval.SpliceSite;
import org.bgi.flexlab.gaea.tools.annotator.interval.Transcript;
import org.bgi.flexlab.gaea.tools.annotator.interval.TranscriptSupportLevel;
import org.bgi.flexlab.gaea.tools.annotator.util.Timer;

public class AnnotatorBuild implements Serializable{
	
	private static final long serialVersionUID = 8558515853505312687L;
	
	protected boolean debug; // Debug mode
	protected boolean verbose; // Be verbose
	protected boolean canonical = false; // Use only canonical transcripts
	protected boolean strict = false; // Only use transcript that have been validated
	
	protected boolean hgvs = true; // Use Hgvs notation
	protected boolean hgvsOneLetterAa = false; // Use 1-letter AA codes in HGVS.p notation?
	protected boolean hgvsShift = true; // Shift variants towards the 3-prime end of the transcript
	protected boolean hgvsTrId = false; // Use full transcript version in HGVS notation?
	
	protected int spliceSiteSize = SpliceSite.CORE_SPLICE_SITE_SIZE; // Splice site size default: 2 bases (canonical splice site)
	protected int spliceRegionExonSize = SpliceSite.SPLICE_REGION_EXON_SIZE;
	protected int spliceRegionIntronMin = SpliceSite.SPLICE_REGION_INTRON_MIN;
	protected int spliceRegionIntronMax = SpliceSite.SPLICE_REGION_INTRON_MAX;
	protected int upDownStreamLength = SnpEffectPredictor.DEFAULT_UP_DOWN_LENGTH; // Upstream & downstream interval length
	
	protected TranscriptSupportLevel maxTranscriptSupportLevel = null; // Filter by maximum Transcript Support Level (TSL)
	protected boolean onlyProtein = false; // Only use protein coding transcripts
	
	private Config config; 
	Properties properties;
	boolean storeAlignments; // Store alignments (used for some test cases)
	boolean storeSequences = false; // Store full sequences
	
	HashMap<String, TableInfo> dbInfo = null;
	
	public AnnotatorBuild(Config config) {
		this.config = config;
	}
	
	/**
	 * Create SnpEffectPredictor
	 */
	SnpEffectPredictor createSnpEffPredictor() {

		// Create factory
		SnpEffPredictorFactory factory = null;
		
//		TODO 支持多种基因信息格式:  RefSeq, EMBL, UCSC KnownGenes ...
		factory = new SnpEffPredictorFactoryRefSeq(config);

		// Create SnpEffPredictors
		factory.setVerbose(config.isVerbose());
		factory.setDebug(config.isDebug());
		factory.setStoreSequences(storeSequences);
		
		
		
		
		return factory.create();
	}
	
	void buildForest(){
		// Set upstream-downstream interval length
		config.getSnpEffectPredictor().setUpDownStreamLength(upDownStreamLength);

		// Set splice site/region sizes
		config.getSnpEffectPredictor().setSpliceSiteSize(spliceSiteSize);
		config.getSnpEffectPredictor().setSpliceRegionExonSize(spliceRegionExonSize);
		config.getSnpEffectPredictor().setSpliceRegionIntronMin(spliceRegionIntronMin);
		config.getSnpEffectPredictor().setSpliceRegionIntronMax(spliceRegionIntronMax);

		// Filter canonical transcripts
		if (canonical) {
			if (verbose) Timer.showStdErr("Filtering out non-canonical transcripts.");
			config.getSnpEffectPredictor().removeNonCanonical();

			if (verbose) {
				// Show genes and transcript (which ones are considered 'canonical')
				Timer.showStdErr("Canonical transcripts:\n\t\tgeneName\tgeneId\ttranscriptId\tcdsLength");
				for (Gene g : config.getSnpEffectPredictor().getGenome().getGenes()) {
					for (Transcript t : g) {
						String cds = t.cds();
						int cdsLen = (cds != null ? cds.length() : 0);
						System.err.println("\t\t" + g.getGeneName() + "\t" + g.getId() + "\t" + t.getId() + "\t" + cdsLen);
					}
				}
			}
			if (verbose) Timer.showStdErr("done.");
		}

		// Filter transcripts by TSL
		if (maxTranscriptSupportLevel != null) {
			if (verbose) Timer.showStdErr("Filtering transcripts by Transcript Support Level (TSL): " + maxTranscriptSupportLevel);
			config.getSnpEffectPredictor().filterTranscriptSupportLevel(maxTranscriptSupportLevel);

			if (verbose) {
				// Show genes and transcript (which ones are considered 'canonical')
				Timer.showStdErr("Transcript:\n\t\tgeneName\tgeneId\ttranscriptId\tTSL");
				for (Gene g : config.getSnpEffectPredictor().getGenome().getGenes()) {
					for (Transcript t : g)
						System.err.println("\t\t" + g.getGeneName() + "\t" + g.getId() + "\t" + t.getId() + "\t" + t.getTranscriptSupportLevel());
				}
			}
			if (verbose) Timer.showStdErr("done.");
		}

		// Filter verified transcripts
		if (strict) {
			if (verbose) Timer.showStdErr("Filtering out non-verified transcripts.");
			if (config.getSnpEffectPredictor().removeUnverified()) {
				fatalError("All transcripts have been removed form every single gene!\nUsing strickt on this database leaves no information.");
			}
			if (verbose) Timer.showStdErr("done.");
		}

		// Use transcripts set form input file
//		if (onlyTranscriptsFile != null) {
//			// Load file
//			String onlyTr = Gpr.readFile(onlyTranscriptsFile);
//			HashSet<String> trIds = new HashSet<String>();
//			for (String trId : onlyTr.split("\n"))
//				trIds.add(trId.trim());
//
//			// Remove transcripts
//			if (verbose) Timer.showStdErr("Filtering out transcripts in file '" + onlyTranscriptsFile + "'. Total " + trIds.size() + " transcript IDs.");
//			int removed = config.getSnpEffectPredictor().retainAllTranscripts(trIds);
//			if (verbose) Timer.showStdErr("Done: " + removed + " transcripts removed.");
//		}

		// Use protein coding transcripts
		if (onlyProtein) {
			// Remove transcripts
			if (verbose) Timer.showStdErr("Filtering out non-protein coding transcripts.");
			int removed = config.getSnpEffectPredictor().retainTranscriptsProtein();
			if (verbose) Timer.showStdErr("Done: " + removed + " transcripts removed.");
		}

		// Build tree
		if (verbose) Timer.showStdErr("Building interval forest");
		config.getSnpEffectPredictor().buildForest();
		if (verbose) Timer.showStdErr("done.");
	}
	
	/**
	 * Show an error message and exit
	 */
	public void fatalError(String message) {
		System.err.println("Fatal error: " + message);
		System.exit(-1);
	}
	
	
	
}
