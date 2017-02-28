package org.bgi.flexlab.gaea.tools.mapreduce.genotyper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.GenotypeLikelihoodCalculator;
import org.bgi.flexlab.gaea.tools.genotyer.VariantCallingEngine;
import org.bgi.flexlab.gaea.tools.genotyer.genotypecaller.AFCalcFactory;
import org.bgi.flexlab.gaea.util.GaeaVariantContextUtils;

import java.util.ArrayList;
import java.util.List;

import static org.bgi.flexlab.gaea.tools.genotyer.VariantCallingEngine.OUTPUT_MODE.EMIT_VARIANTS_ONLY;

/**
 * Created by zhangyong on 2016/12/20.
 */
public class GenotyperOptions extends GaeaOptions implements HadoopOptions {
    private final static String SOFTWARE_NAME = "Genotyper";
    private final static String SOFTWARE_VERSION = "1.0";

    /**
     * input alignment data
     */
    private String input = null;

    /**
     * output directory
     */
    private String output = null;

    /**
     * Which annotations to add to the output VCF file
     */
    private List<String> annotations = new ArrayList<>();

    /**
     * models for genotype likelihood calculator
     */
    private GenotypeLikelihoodCalculator.Model gtlcalculators = GenotypeLikelihoodCalculator.Model.BOTH;

    /**
     * min depth for genotype likelihood calculation
     */
    private int minDepth = 4;

    /**
     * is cap base quality at mapping quality
     */
    private boolean noCapBaseQualsAtMappingQual = false;

    /**
     * minimum base quality
     */
    private byte minBaseQuality = 17;

    /**
     * minimum mapping quality
     */
    private short minMappingQuality = 17;

    /**
     * output mode
     */
    private VariantCallingEngine.OUTPUT_MODE outputMode = EMIT_VARIANTS_ONLY;

    /**
     * A candidate indel is genotyped (and potentially called) if there are this number of reads with a consensus indel at a site.
     * Decreasing this value will increase sensitivity but at the cost of larger calling time and a larger number of false positives.
     */
    private int minIndelCountForGenotyping = 5;

    /**
     * Complementary argument to minIndelCnt.  Only samples with at least this fraction of indel-containing reads will contribute
     * to counting and overcoming the threshold minIndelCnt.  This parameter ensures that in deep data you don't end
     * up summing lots of super rare errors up to overcome the 5 read default threshold.  Should work equally well for
     * low-coverage and high-coverage samples, as low coverage samples with any indel containing reads should easily over
     * come this threshold.
     */
    private double minIndelFractionPerSample = 0.25;

    /**
     * If this fraction is greater is than zero, the caller will aggressively attempt to remove contamination through biased down-sampling of reads.
     * Basically, it will ignore the contamination fraction of reads for each alternate allele.  So if the pileup contains N total bases, then we
     * will try to remove (N * contamination fraction) bases for each alternate allele.
     */
    private double contaminationFraction = DEFAULT_CONTAMINATION_FRACTION;
    public static final double DEFAULT_CONTAMINATION_FRACTION = 0.05;

    /**
     * If there are more than this number of alternate alleles presented to the genotyper (either through discovery or GENOTYPE_GIVEN ALLELES),
     * then only this many alleles will be used.  Note that genotyping sites with many alternate alleles is both CPU and memory intensive and it
     * scales exponentially based on the number of alternate alleles.  Unless there is a good reason to change the default value, we highly recommend
     * that you not play around with this parameter.
     */
    private int maxAlternateAlleles = 6;

    /**
     * Sample ploidy - equivalent to number of chromosomes per pool. In pooled experiments this should be = # of samples in pool * individual sample ploidy
     */
    private int samplePloidy = GaeaVariantContextUtils.DEFAULT_PLOIDY;

    /**
     * allele frequency calculation model
     */
    private AFCalcFactory.Calculation AFmodel = AFCalcFactory.Calculation.getDefaultModel();

    /**
     * The minimum phred-scaled Qscore threshold to separate high confidence from low confidence calls. Only genotypes with
     * confidence >= this threshold are emitted as called sites. A reasonable threshold is 30 for high-pass calling (this
     * is the default).
     */
    public double standardConfidenceForCalling = 30.0;

    /**
     * This argument allows you to emit low quality calls as filtered records.
     */
    public double standardConfidenceForEmitting = 30.0;

    /**
     * The expected heterozygosity value used to compute prior likelihoods for any locus. The default priors are:
     * het = 1e-3, P(hom-ref genotype) = 1 - 3 * het / 2, P(het genotype) = het, P(hom-var genotype) = het / 2
     */
    public Double heterozygosity = VariantCallingEngine.HUMAN_SNP_HETEROZYGOSITY;

    /**
     * This argument informs the prior probability of having an indel at a site.
     */
    public double indelHeterozygosity = 1.0/8000;

    /**
     * Depending on the value of the --max_alternate_alleles argument, we may genotype only a fraction of the alleles being sent on for genotyping.
     * Using this argument instructs the genotyper to annotate (in the INFO field) the number of alternate alleles that were originally discovered at the site.
     */
    public boolean annotateNumberOfAllelesDiscovered = false;

    public GenotyperOptions() {
        addOption("i", "input", true, "Input file containing sequence data (BAM or CRAM)");
        addOption("o", "output", true, "directory to which variants should be written in HDFS written format.");
        addOption("A", "annotation", true, "One or more specific annotations to apply to variant calls, the tag is separated by \',\'");

        addOption("glm", "genotypeLikelihoodModel", true, "models for genotype likelihood calculation: SNP, INDEL, BOTH.");
        addOption("D", "minDepth", true, "minimum depth for variant calling.");
        addOption("C", "noCapBaseQualsAtMappingQual", false, "do not cap base quality at mapping quality");
        addOption("B", "minBaseQuality", true, "minimum base quality.");
        addOption("M", "minMappingQuality", true, "minimum mapping quality.");
        addOption("O", "outputMode", true, "output mode :EMIT_VARIANTS_ONLY, EMIT_ALL_CONFIDENT_SITES, EMIT_ALL_SITES");
        addOption("X", "minIndelCountForGenotyping", true, "minimum indel count for genotyping in all samples.");
        addOption("Y", "minIndelFractionPerSample", true, "minimum indel fraction for genotyping in each sample.");
        addOption("C1", "contaminationFraction", true, "contamination fraction.");
        addOption("M1", "maxAlternateAlleles", true, "max alternate alleles");
        addOption("P", "samplePloidy", true, "sample ploidy");
        addOption("A", "AFmodel", true, "allele frequency calculation model");
        addOption("C1", "standardConfidenceForCalling", true, "standard confidence for calling");
        addOption("C2", "standardConfidenceForEmitting", true, "standard confidence for emitting");
        addOption("H", "heterozygosity", true, "heterozygosity for SNP");
        addOption("H1", "indelHeterozygosity", true, "indel heterozygosity");
        addOption("A1", "annotateNumberOfAllelesDiscovered", false, "annotate Number Of Alleles Discovered");
    }

    @Override
    public void parse(String[] args) {
        input = getOptionValue("i", null);
        output = getOptionValue("o", null);
        String annotaionTags = getOptionValue("A", null);
        if(annotaionTags != null) {
            for(String tag : annotaionTags.split(","))
            annotations.add(tag);
        }

        minDepth = getOptionIntValue("D", 4);
        noCapBaseQualsAtMappingQual = getOptionBooleanValue("C", false);
        minBaseQuality = getOptionByteValue("B", (byte)17);
        minMappingQuality = getOptionShortValue("M", (short)17);
        minIndelCountForGenotyping = getOptionIntValue("X", 5);
        minIndelFractionPerSample = getOptionDoubleValue("Y", 0.25);
        contaminationFraction = getOptionDoubleValue("C1", DEFAULT_CONTAMINATION_FRACTION);
        maxAlternateAlleles = getOptionIntValue("M1", 6);
        samplePloidy = getOptionIntValue("P", GaeaVariantContextUtils.DEFAULT_PLOIDY);
        standardConfidenceForCalling = getOptionDoubleValue("C1", 30);
        heterozygosity = getOptionDoubleValue("H", VariantCallingEngine.HUMAN_SNP_HETEROZYGOSITY);
        indelHeterozygosity = getOptionDoubleValue("H1", 1.0/8000);
        annotateNumberOfAllelesDiscovered = getOptionBooleanValue("A1", false);

        try {
            gtlcalculators = GenotypeLikelihoodCalculator.Model.valueOf(getOptionValue("glm", "BOTH"));
        } catch (Exception e) {
            throw new UserException.BadArgumentValueException("glm", e.getMessage());
        }
        try {
            outputMode = VariantCallingEngine.OUTPUT_MODE.valueOf(getOptionValue("O", "EMIT_VARIANTS_ONLY"));
        } catch (Exception e) {
            throw new UserException.BadArgumentValueException("O", e.getMessage());
        }

        try {
            AFmodel = AFCalcFactory.Calculation.valueOf(getOptionValue("O", "EMIT_VARIANTS_ONLY"));
        } catch (Exception e) {
            throw new UserException.BadArgumentValueException("O", e.getMessage());
        }

        check();
    }

    private void check() {
        if(input == null)
            throw new UserException.BadArgumentValueException("i", "input directory or file is not assigned.");

        if(output == null)
            throw new UserException.BadArgumentValueException("o", "output directory is not assigned.");
    }

    @Override
    public void setHadoopConf(String[] args, Configuration conf) {
        conf.setStrings("args", args);
    }

    @Override
    public void getOptionsFromHadoopConf(Configuration conf) {
        String[] args = conf.getStrings("args");
        this.parse(args);
    }

    public GenotypeLikelihoodCalculator.Model getGtlcalculators() {
        return gtlcalculators;
    }

    public void setGtlcalculators(GenotypeLikelihoodCalculator.Model gtlcalculators) {
        this.gtlcalculators = gtlcalculators;
    }

    public int getMinDepth() {
        return minDepth;
    }

    public void setMinDepth(int minDepth) {
        this.minDepth = minDepth;
    }

    public boolean isCapBaseQualsAtMappingQual() {
        return !noCapBaseQualsAtMappingQual;
    }

    public byte getMinBaseQuality() {
        return minBaseQuality;
    }

    public short getMinMappingQuality() {
        return minMappingQuality;
    }

    public VariantCallingEngine.OUTPUT_MODE getOutputMode() {
        return outputMode;
    }

    public int getMinIndelCountForGenotyping() {
        return minIndelCountForGenotyping;
    }

    public double getMinIndelFractionPerSample() {
        return minIndelFractionPerSample;
    }

    public double getContaminationFraction() {
        return contaminationFraction;
    }

    public int getMaxAlternateAlleles() {
        return maxAlternateAlleles;
    }

    public int getSamplePloidy() {
        return samplePloidy;
    }

    public AFCalcFactory.Calculation getAFmodel() {
        return AFmodel;
    }

    public void setAFmodel(AFCalcFactory.Calculation AFmodel) {
        this.AFmodel = AFmodel;
    }

    public double getStandardConfidenceForCalling() {
        return standardConfidenceForCalling;
    }

    public double getStandardConfidenceForEmitting() {
        return standardConfidenceForEmitting;
    }

    public double getHeterozygosity() {
        return heterozygosity;
    }

    public double getIndelHeterozygosity() {
        return indelHeterozygosity;
    }

    public boolean isAnnotateNumberOfAllelesDiscovered() {
        return annotateNumberOfAllelesDiscovered;
    }
}
