package org.bgi.flexlab.gaea.tools.annotator;

import java.util.Properties;

import org.bgi.flexlab.gaea.tools.annotator.codons.CodonTable;
import org.bgi.flexlab.gaea.tools.annotator.codons.CodonTables;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.effect.SnpEffectPredictor;
import org.bgi.flexlab.gaea.tools.annotator.effect.factory.SnpEffPredictorFactory;
import org.bgi.flexlab.gaea.tools.annotator.effect.factory.SnpEffPredictorFactoryRefSeq;
import org.bgi.flexlab.gaea.tools.annotator.interval.Chromosome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Genome;

public class AnnotatorBuild {

	private Config config; 
	boolean storeAlignments; // Store alignments (used for some test cases)
	boolean storeSequences = false; // Store full sequences
	
	public AnnotatorBuild(){
		
	}
	
	public AnnotatorBuild(Config userConfig) {
		config = userConfig;
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
	
	
	/**
	 * Extract and create codon tables
	 */
	void createCodonTables(String genomeId, Properties properties) {
		//---
		// Read codon tables
		//---
		for (Object key : properties.keySet()) {
			if (key.toString().startsWith(KEY_CODON_PREFIX)) {
				String name = key.toString().substring(KEY_CODON_PREFIX.length());
				String table = properties.getProperty(key.toString());
				CodonTable codonTable = new CodonTable(name, table);
				CodonTables.getInstance().add(codonTable);
			}
		}

		//---
		// Assign codon tables for different genome+chromosome
		//---
		for (Object key : properties.keySet()) {
			String keyStr = key.toString();
			if (keyStr.endsWith(KEY_CODONTABLE_SUFIX) && keyStr.startsWith(genomeId + ".")) {
				// Everything between gneomeName and ".codonTable" is assumed to be chromosome name
				int chrNameEnd = keyStr.length() - KEY_CODONTABLE_SUFIX.length();
				int chrNameStart = genomeId.length() + 1;
				int chrNameLen = chrNameEnd - chrNameStart;
				String chromo = null;
				if (chrNameLen > 0) chromo = keyStr.substring(chrNameStart, chrNameEnd);

				// Find codon table
				String codonTableName = properties.getProperty(key.toString());
				CodonTable codonTable = CodonTables.getInstance().getTable(codonTableName);
				if (codonTable == null) throw new RuntimeException("Error parsing property '" + key + "'. No such codon table '" + codonTableName + "'");

				// Find genome
				Genome gen = getGenome(genomeId);
				if (gen == null) throw new RuntimeException("Error parsing property '" + key + "'. No such genome '" + genomeId + "'");

				if (chromo != null) {
					// Find chromosome
					Chromosome chr = gen.getOrCreateChromosome(chromo);

					// Everything seems to be OK, go on
					CodonTables.getInstance().set(genomeById.get(genomeId), chr, codonTable);
				} else {
					// Set genome-wide chromosome table
					CodonTables.getInstance().set(genomeById.get(genomeId), codonTable);
				}
			}
		}
	}
	
}
