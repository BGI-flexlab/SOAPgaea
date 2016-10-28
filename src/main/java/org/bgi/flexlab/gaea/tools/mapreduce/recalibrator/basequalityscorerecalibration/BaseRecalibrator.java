package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.basequalityscorerecalibration;

import htsjdk.samtools.SAMFileHeader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker.Style;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationDatum;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationTables;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationUtils;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.ReadGroupCovariate;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;
import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;
import org.bgi.flexlab.gaea.util.NestedIntegerArray.Leaf;
import org.bgi.flexlab.gaea.util.Pair;
import org.bgi.flexlab.gaea.util.ReadUtils;
import org.bgi.flexlab.gaea.util.Window;


public class BaseRecalibrator {

	private BaseRecalibrationOptions RAC = null;
	private RecalibrationTables recalibrationTables;
	private SAMFileHeader mFileHeader = null;
	private Covariate[] requestedCovariates;
	private RecalibrationEngine recalibrationEngine;
	private int minimumQToUse;
	private Style s = Style.POS;
	private List<VariantDataTracker> knowSite = null;
	protected static final String SKIP_RECORD_ATTRIBUTE = "SKIP"; // used to label reads that should be skipped.
	protected static final String SEEN_ATTRIBUTE = "SEEN"; // used to label reads as processed.
	protected static final String COVARS_ATTRIBUTE = "COVARS"; // used to store covariates array as a
																// temporary attribute inside GATKSAMRecord.\

	public BaseRecalibrator(SAMFileHeader mFileHeader, BaseRecalibrationOptions RAC) {
		this.mFileHeader = mFileHeader;
		this.RAC = RAC;
	}
	
	public void initKnowSite(Window exwin) {
		if (RAC.getKnowSite() != null) {
			if(knowSite == null)
				knowSite = new ArrayList<VariantDataTracker>();
			else
				knowSite.clear();
			for (String site : RAC.getKnowSite()) {
				VariantDataTracker tracker = new VariantDataTracker(site,
						"dbsnp", exwin, s);
				knowSite.add(tracker);
			}
		}
	}
	
	public void initialize(){
		parseCovariate();
		int numReadGroups = mFileHeader.getReadGroups().size();
		recalibrationTables = new RecalibrationTables(requestedCovariates,
				numReadGroups);
		recalibrationEngine = new RecalibrationEngine();
		recalibrationEngine
				.initialize(requestedCovariates, recalibrationTables);
		minimumQToUse = RAC.PRESERVE_QSCORES_LESS_THAN;
	}

	public void map(byte refBase, Pileup context) {
		if (!knownSite(context)) { // Only analyze sites not present in the provided known sites
			for (final PileupElement p : context) {
				final GaeaSamRecord read = p.getRead();
				final int offset = p.getOffset();
				if (readHasBeenSkipped(read) || isLowQualityBase(read, offset)
						|| isDeletionOffset(p)) // This read has been marked to be skipped or base is low
												// quality (we don't recalibrate low quality bases)
					continue;

				if (readNotSeen(read)) {
					read.setTemporaryAttribute(SEEN_ATTRIBUTE, true);
					RecalibrationUtils.parsePlatformForRead(read, RAC);
					if (!RecalibrationUtils.isColorSpaceConsistent(
							RAC.SOLID_NOCALL_STRATEGY, read)) {
						read.setTemporaryAttribute(SKIP_RECORD_ATTRIBUTE, true);
						continue;
					}
					read.setTemporaryAttribute(COVARS_ATTRIBUTE, RecalibrationUtils
							.computeCovariates(read, requestedCovariates));
				}

				if (!ReadUtils.isSOLiDRead(read)
						|| // SOLID bams have inserted the reference base into
							// the read if the color space in inconsistent with
							// the read base so skip it
						RAC.SOLID_RECAL_MODE == RecalibrationUtils.SOLID_RECAL_MODE.DO_NOTHING
						|| RecalibrationUtils.isColorSpaceConsistent(read, offset))
					recalibrationEngine.updateDataForPileupElement(p, refBase); // This base finally passed all the checks for a
																				// good base, so add it to the big data hash map
			}
		}
	}

	public void print(Context context) {
		for (int i = 0; i < recalibrationEngine.covariates.length; i++) {
			NestedIntegerArray<RecalibrationDatum> covRecalTable = recalibrationTables
					.getTable(i);
			List<Leaf> covLeaf = covRecalTable.getAllLeaves();
			for (Leaf l : covLeaf) {
				printTableAtr(i, l.keys, (RecalibrationDatum) l.value, context);
			}
		}
	}
	
	private void printTableAtr(int tableIndex, int[] keys, RecalibrationDatum recalData, Context context) {

		StringBuilder sb = new StringBuilder();
		sb.append(tableIndex);
		sb.append("\t");
		for (int k : keys) {
			sb.append(k);
			sb.append("\t");
		}
		sb.append(recalData.toString());
		try {
			context.write(NullWritable.get(), new Text(sb.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	
	private void parseCovariate() {
		Pair<ArrayList<Covariate>, ArrayList<Covariate>> covariates = RecalibrationUtils
				.initializeCovariates(RAC); // initialize the required and
											// optional covariates
		ArrayList<Covariate> requiredCovariates = covariates.getFirst();
		ArrayList<Covariate> optionalCovariates = covariates.getSecond();

		requestedCovariates = new Covariate[requiredCovariates.size()
				+ optionalCovariates.size()];
		int covariateIndex = 0;
		for (final Covariate covariate : requiredCovariates)
			requestedCovariates[covariateIndex++] = covariate;
		for (final Covariate covariate : optionalCovariates)
			requestedCovariates[covariateIndex++] = covariate;

		for (Covariate cov : requestedCovariates) { // list all the covariates
													// being used
			cov.initialize(RAC); // initialize any covariate member variables
									// using the shared argument collection
			if (cov instanceof ReadGroupCovariate)
				((ReadGroupCovariate) cov).initializeReadGroup(mFileHeader);

		}
	}
	
	private boolean knownSite(Pileup context) {
		if (knowSite != null) {
			for (VariantDataTracker site : knowSite) {
				if (site.getValue(context.getLocation()))
					return true;
			}
		}
		return false;
	}
	
	private boolean readHasBeenSkipped(GaeaSamRecord read) {
		return read.containsTemporaryAttribute(SKIP_RECORD_ATTRIBUTE);
	}

	private boolean isLowQualityBase(GaeaSamRecord read, int offset) {
		return read.getBaseQualities()[offset] < minimumQToUse;
	}

	private boolean isDeletionOffset(PileupElement p) {
		return p.getBase() == BaseUtils.D;
	}

	private boolean readNotSeen(GaeaSamRecord read) {
		return !read.containsTemporaryAttribute(SEEN_ATTRIBUTE);
	}

}
