package org.bgi.flexlab.gaea.tools.mapreduce.genotyper;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.GenotypeLikelihoodCalculator;
import org.bgi.flexlab.gaea.tools.genotyer.VariantCallingEngine;
import org.omg.PortableInterceptor.USER_EXCEPTION;

/**
 * Created by zhangyong on 2016/12/20.
 */
public class GenotyperOptions extends GaeaOptions implements HadoopOptions {
    private final static String SOFTWARE_NAME = "Genotyper";
    private final static String SOFTWARE_VERSION = "1.0";


    /**
     * models for genotype likelihood calculator
     */
    private String gtlcalculators = "BOTH";

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
    private String outputMode = "EMIT_VARIANTS_ONLY";

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

    public GenotyperOptions() {
        addOption("glm", "genotypeLikelihoodModel", true, "models for genotype likelihood calculation: SNP, INDEL, BOTH.");
        addOption("D", "minDepth", true, "minimum depth for variant calling.");
        addOption("C", "noCapBaseQualsAtMappingQual", false, "do not cap base quality at mapping quality");
        addOption("B", "minBaseQuality", true, "minimum base quality.");
        addOption("M", "minMappingQuality", true, "minimum mapping quality.");
        addOption("O", "outputMode", true, "output mode :EMIT_VARIANTS_ONLY, EMIT_ALL_CONFIDENT_SITES, EMIT_ALL_SITES");
        addOption("X", "minIndelCountForGenotyping", true, "minimum indel count for genotyping in all samples.");
        addOption("Y", "minIndelFractionPerSample", true, "minimum indel fraction for genotyping in each sample.");
        addOption("C1", "contaminationFraction", true, "contamination fraction.");
    }

    @Override
    public void parse(String[] args) {
        gtlcalculators = getOptionValue("glm", "BOTH");
        try {
            GenotypeLikelihoodCalculator.Model.valueOf(gtlcalculators);
        } catch (Exception e) {
            throw new UserException.BadArgumentValueException("glm", e.getMessage());
        }

        minDepth = getOptionIntValue("D", 4);
        noCapBaseQualsAtMappingQual = getOptionBooleanValue("C", false);
        minBaseQuality = getOptionByteValue("B", (byte)17);
        minMappingQuality = getOptionShortValue("M", (short)17);
        outputMode = getOptionValue("O", "EMIT_VARIANTS_ONLY");
        minIndelCountForGenotyping = getOptionIntValue("X", 5);
        minIndelFractionPerSample = getOptionDoubleValue("Y", 0.25);
        contaminationFraction = getOptionDoubleValue("C1", DEFAULT_CONTAMINATION_FRACTION);

        try {
            VariantCallingEngine.OUTPUT_MODE.valueOf(outputMode);
        } catch (Exception e) {
            throw new UserException.BadArgumentValueException("O", e.getMessage());
        }
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
        return GenotypeLikelihoodCalculator.Model.valueOf(gtlcalculators);
    }

    public void setGtlcalculators(String gtlcalculators) {
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
        return VariantCallingEngine.OUTPUT_MODE.valueOf(outputMode);
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
}
