package org.bgi.flexlab.gaea.tools.annotator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo;
import org.bgi.flexlab.gaea.tools.annotator.effect.SnpEffectPredictor;
import org.bgi.flexlab.gaea.tools.annotator.effect.factory.SnpEffPredictorFactory;
import org.bgi.flexlab.gaea.tools.annotator.effect.factory.SnpEffPredictorFactoryRefSeq;

public class AnnotatorBuild implements Serializable{
	
	private static final long serialVersionUID = 8558515853505312687L;

	boolean debug = false; // Debug mode?
	boolean verbose = false; // Verbose
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
	
	
	
}
