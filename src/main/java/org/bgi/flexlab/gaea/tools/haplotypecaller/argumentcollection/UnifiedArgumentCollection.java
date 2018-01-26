package org.bgi.flexlab.gaea.tools.haplotypecaller.argumentcollection;

import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker.Style;
import org.bgi.flexlab.gaea.tools.jointcalling.UnifiedGenotypingEngine;
import org.bgi.flexlab.gaea.util.pairhmm.PairHMM;

import htsjdk.variant.variantcontext.VariantContextUtils;

public class UnifiedArgumentCollection extends StandardCallerArgumentCollection {

	public UnifiedGenotypingEngine.Model GLmodel = UnifiedGenotypingEngine.Model.SNP;

	/**
	 * The PCR error rate is independent of the sequencing error rate, which is
	 * necessary because we cannot necessarily distinguish between PCR errors
	 * vs. sequencing errors. The practical implication for this value is that
	 * it effectively acts as a cap on the base qualities.
	 */
	public Double PCR_error = 1e-4;

	/**
	 * Note that calculating the SLOD increases the runtime by an appreciable
	 * amount.
	 */
	public boolean COMPUTE_SLOD = false;

	/**
	 * Depending on the value of the --max_alternate_alleles argument, we may
	 * genotype only a fraction of the alleles being sent on for genotyping.
	 * Using this argument instructs the genotyper to annotate (in the INFO
	 * field) the number of alternate alleles that were originally discovered at
	 * the site.
	 */
	public boolean ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED = false;

	/**
	 * The PairHMM implementation to use for -glm INDEL genotype likelihood
	 * calculations. The various implementations balance a tradeoff of accuracy
	 * and runtime.
	 */
	public PairHMM.HMM_IMPLEMENTATION pairHMM = PairHMM.HMM_IMPLEMENTATION.ORIGINAL;

	/**
	 * The minimum confidence needed in a given base for it to be used in
	 * variant calling. Note that the base quality of a base is capped by the
	 * mapping quality so that bases on reads with low mapping quality may get
	 * filtered out depending on this value. Note too that this argument is
	 * ignored in indel calling. In indel calling, low-quality ends of reads are
	 * clipped off (with fixed threshold of Q20).
	 */
	public int MIN_BASE_QUALTY_SCORE = 17;

	public Double MAX_DELETION_FRACTION = 0.05;

	// indel-related arguments
	/**
	 * A candidate indel is genotyped (and potentially called) if there are this
	 * number of reads with a consensus indel at a site. Decreasing this value
	 * will increase sensitivity but at the cost of larger calling time and a
	 * larger number of false positives.
	 */
	public int MIN_INDEL_COUNT_FOR_GENOTYPING = 5;

	/**
	 * Complementary argument to minIndelCnt. Only samples with at least this
	 * fraction of indel-containing reads will contribute to counting and
	 * overcoming the threshold minIndelCnt. This parameter ensures that in deep
	 * data you don't end up summing lots of super rare errors up to overcome
	 * the 5 read default threshold. Should work equally well for low-coverage
	 * and high-coverage samples, as low coverage samples with any indel
	 * containing reads should easily over come this threshold.
	 */
	public double MIN_INDEL_FRACTION_PER_SAMPLE = 0.25;

	/**
	 * This argument informs the prior probability of having an indel at a site.
	 */
	public double INDEL_HETEROZYGOSITY = 1.0 / 8000;

	public byte INDEL_GAP_CONTINUATION_PENALTY = 10;

	public byte INDEL_GAP_OPEN_PENALTY = 45;

	public int INDEL_HAPLOTYPE_SIZE = 80;

	// public boolean OUTPUT_DEBUG_INDEL_INFO = false;

	public boolean IGNORE_SNP_ALLELES = false;

	/*
	 * Generalized ploidy argument (debug only): squash all reads into a single
	 * pileup without considering sample info
	 */
	public boolean TREAT_ALL_READS_AS_SINGLE_POOL = false;

	/*
	 * Generalized ploidy argument (debug only): When building site error
	 * models, ignore lane information and build only sample-level error model
	 */
	public boolean IGNORE_LANE_INFO = false;

	/*
	 * Generalized ploidy argument: VCF file that contains truth calls for
	 * reference sample. If a reference sample is included through argument
	 * -refsample, then this argument is required.
	 */
	public VariantDataTracker referenceSampleRod = new VariantDataTracker();

	/*
	 * Reference sample name: if included, a site-specific error model will be
	 * built in order to improve calling quality. This requires ideally that a
	 * bar-coded reference sample be included with the polyploid/pooled data in
	 * a sequencing experimental design. If argument is absent, no per-site
	 * error model is included and calling is done with a generalization of
	 * traditional statistical calling.
	 */
	public String referenceSampleName;

	/*
	 * Sample ploidy - equivalent to number of chromosomes per pool. In pooled
	 * experiments this should be = # of samples in pool * individual sample
	 * ploidy
	 */
	public int samplePloidy = VariantContextUtils.DEFAULT_PLOIDY;

	byte minQualityScore = 1;

	byte maxQualityScore = 40;

	byte phredScaledPrior = 20;

	double minPower = 0.95;

	int minReferenceDepth = 100;

	public Style s = Style.ALL;

	boolean EXCLUDE_FILTERED_REFERENCE_SITES = false;

	// @Argument(fullName = "baq", shortName="baq", doc="Type of BAQ calculation
	// to apply in the engine", required = false)
	public BAQ.CalculationMode BAQMode = BAQ.CalculationMode.OFF;
	// @Argument(fullName = "baqGapOpenPenalty", shortName="baqGOP", doc="BAQ
	// gap open penalty (Phred Scaled). Default value is 40. 30 is perhaps
	// better for whole genome call sets", required = false)
	public double BAQGOP = BAQ.DEFAULT_GOP;

	// add

	/**
	 * Create a new UAC with defaults for all UAC arguments
	 */
	public UnifiedArgumentCollection() {
		super();
	}

	/**
	 * Create a new UAC based on the information only our in super-class scac
	 * and defaults for all UAC arguments
	 * 
	 * @param scac
	 */
	public UnifiedArgumentCollection(final StandardCallerArgumentCollection scac) {
		super(scac);
	}

	/**
	 * Create a new UAC with all parameters having the values in uac
	 *
	 * @param uac
	 */
	public UnifiedArgumentCollection(final UnifiedArgumentCollection uac) {
		// Developers must remember to add any newly added arguments to the list
		// here as well otherwise they won't get changed from their default
		// value!
		super(uac);

		this.GLmodel = uac.GLmodel;
		this.AFmodel = uac.AFmodel;
		this.PCR_error = uac.PCR_error;
		this.COMPUTE_SLOD = uac.COMPUTE_SLOD;
		this.ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED = uac.ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED;
		this.MIN_BASE_QUALTY_SCORE = uac.MIN_BASE_QUALTY_SCORE;
		this.MAX_DELETION_FRACTION = uac.MAX_DELETION_FRACTION;
		this.MIN_INDEL_COUNT_FOR_GENOTYPING = uac.MIN_INDEL_COUNT_FOR_GENOTYPING;
		this.MIN_INDEL_FRACTION_PER_SAMPLE = uac.MIN_INDEL_FRACTION_PER_SAMPLE;
		this.INDEL_HETEROZYGOSITY = uac.INDEL_HETEROZYGOSITY;
		this.INDEL_GAP_OPEN_PENALTY = uac.INDEL_GAP_OPEN_PENALTY;
		this.INDEL_GAP_CONTINUATION_PENALTY = uac.INDEL_GAP_CONTINUATION_PENALTY;
		// this.OUTPUT_DEBUG_INDEL_INFO = uac.OUTPUT_DEBUG_INDEL_INFO;
		this.INDEL_HAPLOTYPE_SIZE = uac.INDEL_HAPLOTYPE_SIZE;
		this.TREAT_ALL_READS_AS_SINGLE_POOL = uac.TREAT_ALL_READS_AS_SINGLE_POOL;
		this.referenceSampleRod = uac.referenceSampleRod;
		this.referenceSampleName = uac.referenceSampleName;
		this.samplePloidy = uac.samplePloidy;
		this.maxQualityScore = uac.minQualityScore;
		this.phredScaledPrior = uac.phredScaledPrior;
		this.minPower = uac.minPower;
		this.minReferenceDepth = uac.minReferenceDepth;
		this.EXCLUDE_FILTERED_REFERENCE_SITES = uac.EXCLUDE_FILTERED_REFERENCE_SITES;
		this.IGNORE_LANE_INFO = uac.IGNORE_LANE_INFO;
		this.pairHMM = uac.pairHMM;
		this.BAQMode = uac.BAQMode;
		// todo- arguments to remove
		this.IGNORE_SNP_ALLELES = uac.IGNORE_SNP_ALLELES;
	}

	public void initialize(Parameter para, Window window) {
		if (para.getAlleles() == null)
			this.alleles = null;
		else
			this.alleles = new VariantDataTracker(para.getAlleles(), "alleles", window, s);

		this.ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED = para.isAnnotateNDA();
		// annotion -A
		// comp -comp
		this.COMPUTE_SLOD = para.isComputeSLOD();
		this.CONTAMINATION_FRACTION = para.getContamination();
		// dbsnp ----------
		// excludeAnnotation -XA

		setGLmodel(para.getGenotype_likelihoods_model());
		setGModel(para.getGenotyping_mode());
		// group -G
		this.heterozygosity = para.getHeterozygosity();
		this.INDEL_HETEROZYGOSITY = para.getIndel_heterozygosity();
		this.MAX_DELETION_FRACTION = para.getMax_deletion_fraction();
		this.MIN_BASE_QUALTY_SCORE = para.getMin_base_quality_score();
		this.MIN_INDEL_COUNT_FOR_GENOTYPING = para.getMinIndelCnt();
		this.MIN_INDEL_FRACTION_PER_SAMPLE = para.getMinIndelFrac();
		setOutputModel(para.getOutput_mode());
		setPairHMM(para.getPair_hmm_implementation());

		this.PCR_error = para.getPcr_error_rate();
		this.samplePloidy = para.getSample_ploidy();
		this.STANDARD_CONFIDENCE_FOR_CALLING = para.getStand_call_conf();
		this.STANDARD_CONFIDENCE_FOR_EMITTING = para.getStand_emit_conf();
		// -allSitePLs
		// contaminationFile
		this.INDEL_GAP_CONTINUATION_PENALTY = para.getIndelGapContinuationPenalty();
		this.INDEL_GAP_OPEN_PENALTY = para.getIndelGapOpenPenalty();
		// input_prior
		this.MAX_ALTERNATE_ALLELES = para.getMax_alternate_alleles();
		// add============
		this.referenceSampleName = para.getRefsample();
		if (para.getReferenceCalls() != null)
			this.referenceSampleRod = new VariantDataTracker(para.getReferenceCalls(), "referenceCalls", window, s);
		this.IGNORE_LANE_INFO = para.isIgnoreLane();
	}

	private void setOutputModel(String model) {
		/** produces calls only at variant sites */
		// EMIT_VARIANTS_ONLY,
		/** produces calls at variant sites and confident reference sites */
		// EMIT_ALL_CONFIDENT_SITES,
		/**
		 * produces calls at any callable site regardless of confidence; this
		 * argument is intended only for point mutations (SNPs) in DISCOVERY
		 * mode or generally when running in GENOTYPE_GIVEN_ALLELES mode; it
		 * will by no means produce a comprehensive set of indels in DISCOVERY
		 * mode
		 */
		// EMIT_ALL_SITES
		if (model.equals("EMIT_VARIANTS_ONLY"))
			this.OutputMode = UnifiedGenotyperEngine.OUTPUT_MODE.EMIT_VARIANTS_ONLY;
		else if (model.equals("EMIT_ALL_CONFIDENT_SITES"))
			this.OutputMode = UnifiedGenotyperEngine.OUTPUT_MODE.EMIT_ALL_CONFIDENT_SITES;
		else if (model.equals("EMIT_ALL_SITES"))
			this.OutputMode = UnifiedGenotyperEngine.OUTPUT_MODE.EMIT_ALL_SITES;

	}

	private void setGModel(String model) {
		/**
		 * the Unified Genotyper will choose the most likely alternate allele
		 */
		// DISCOVERY,
		/**
		 * only the alleles passed in from a VCF rod bound to the -alleles
		 * argument will be used for genotyping
		 */
		// GENOTYPE_GIVEN_ALLELES
		if (model.equals("DISCOVERY"))
			this.GenotypingMode = GenotypeLikelihoodsCalculationModel.GENOTYPING_MODE.DISCOVERY;
		// else if(model.equals("GENOTYPE_GIVEN_ALLELES"))
		// this.GenotypingMode=GenotypeLikelihoodsCalculationModel.GENOTYPING_MODE.GENOTYPE_GIVEN_ALLELES;

	}

	private void setPairHMM(String hmm) {
		/**
		 * 
		 * EXACT, ORIGINAL, CACHING, LOGLESS_CACHING
		 */
		if (hmm.equals("EXACT"))
			pairHMM = PairHMM.HMM_IMPLEMENTATION.EXACT;
		else if (hmm.equals("ORIGINAL"))
			pairHMM = PairHMM.HMM_IMPLEMENTATION.ORIGINAL;
		else if (hmm.equals("CACHING"))
			pairHMM = PairHMM.HMM_IMPLEMENTATION.CACHING;
		else if (hmm.equals("LOGLESS_CACHING"))
			pairHMM = PairHMM.HMM_IMPLEMENTATION.LOGLESS_CACHING;
		else {
			System.out.println("unknow pairHMM model:" + hmm + ".we will use default model[ORIGINAL]");
		}
	}

	private void setGLmodel(String model) {
		/**
		 * 
		 * SNP, INDEL, GeneralPloidySNP, GeneralPloidyINDEL, BOTH
		 */
		if (model.equals("SNP"))
			GLmodel = UnifiedGenotypingEngine.Model.SNP;
		else if (model.equals("INDEL"))
			GLmodel = UnifiedGenotypingEngine.Model.INDEL;
		else if (model.equals("GeneralPloidySNP"))
			GLmodel = UnifiedGenotypingEngine.Model.GENERALPLOIDYSNP;
		else if (model.equals("GeneralPloidyINDEL"))
			GLmodel = UnifiedGenotypingEngine.Model.GENERALPLOIDYINDEL;
		else if (model.equals("BOTH"))
			GLmodel = UnifiedGenotypingEngine.Model.BOTH;
		else {
			System.out.println("unknow GLmodel:" + model + ".we will use default model[SNP]");
			GLmodel = UnifiedGenotypingEngine.Model.SNP;
		}
	}

}
