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
    private String gtlcalculators;

    /**
     * min depth for genotype likelihood calculation
     */
    private int minDepth;

    /**
     * is cap base quality at mapping quality
     */
    private boolean noCapBaseQualsAtMappingQual;

    /**
     * minimum base quality
     */
    private byte minBaseQuality;

    /**
     * minimum mapping quality
     */
    private short minMappingQuality;

    /**
     * output mode
     */
    private String outputMode;

    public GenotyperOptions() {
        addOption("glm", "genotypeLikelihoodModel", true, "models for genotype likelihood calculation: SNP, INDEL, BOTH.");
        addOption("D", "minDepth", true, "minimum depth for variant calling.");
        addOption("C", "noCapBaseQualsAtMappingQual", false, "do not cap base quality at mapping quality");
        addOption("B", "minBaseQuality", true, "minimum base quality.");
        addOption("M", "minMappingQuality", true, "minimum mapping quality.");
        addOption("O", "outputMode", true, "output mode :EMIT_VARIANTS_ONLY, EMIT_ALL_CONFIDENT_SITES, EMIT_ALL_SITES");
    }

    @Override
    public void parse(String[] args) {
        gtlcalculators = getOptionValue("glm", "SNP");
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
}
