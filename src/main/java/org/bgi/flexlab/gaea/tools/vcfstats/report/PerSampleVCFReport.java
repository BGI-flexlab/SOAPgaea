package org.bgi.flexlab.gaea.tools.vcfstats.report;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import org.bgi.flexlab.gaea.tools.vcfstats.VariantType;
import org.bgi.flexlab.gaea.util.Histogram;
import org.bgi.flexlab.gaea.util.Pair;
import org.bgi.flexlab.gaea.util.StatsUtils;

import java.util.ArrayList;
import java.util.List;

public class PerSampleVCFReport {

    private static final String[] VARIANT_TYPE_NAMES = {
            "No-call", "Reference", "SNP", "MNP", "Delete", "Insert", "MIXED", "Breakend", "Symbolic"
    };

    private String sampleName;

    protected long mTotalUnchanged = 0;
    protected long mHeterozygous = 0;
    protected long mHomozygous = 0;
    protected long mNoCall = 0;
    protected long mDeNovo = 0;
    protected long mPhased = 0;

    protected long mTotalSnps = 0;
    protected long mTransitions = 0;
    protected long mTransversions = 0;
    protected long mHeterozygousSnps = 0;
    protected long mHomozygousSnps = 0;

    protected long mTotalMnps = 0;
    protected long mHeterozygousMnps = 0;
    protected long mHomozygousMnps = 0;

    protected long mTotalMixeds = 0;
    protected long mHeterozygousMixeds = 0;
    protected long mHomozygousMixeds = 0;

    protected long mTotalInsertions = 0;
    protected long mHeterozygousInsertions = 0;
    protected long mHomozygousInsertions = 0;

    protected long mTotalDeletions = 0;
    protected long mHeterozygousDeletions = 0;
    protected long mHomozygousDeletions = 0;

    protected long mTotalBreakends = 0;
    protected long mHeterozygousBreakends = 0;
    protected long mHomozygousBreakends = 0;

    protected final Histogram[] mAlleleLengths;


    public PerSampleVCFReport(){
        sampleName = null;
        mAlleleLengths = new Histogram[VARIANT_TYPE_NAMES.length];
        for (int i = VariantContext.Type.SNP.ordinal(); i < mAlleleLengths.length; ++i) {
            // i from SNP as we don't care about NO_CALL/UNCHANGED
            mAlleleLengths[i] = new Histogram();
        }
    }

    public void parseReducerString(String reducerStr){
        String[] fields = reducerStr.split("\t");
        if(sampleName == null)
            sampleName = fields[0];

        mTotalUnchanged += Integer.valueOf(fields[1]);
        mHeterozygous += Integer.valueOf(fields[2]);
        mHomozygous += Integer.valueOf(fields[3]);
        mNoCall += Integer.valueOf(fields[4]);
        mDeNovo += Integer.valueOf(fields[5]);
        mPhased += Integer.valueOf(fields[6]);

        mTotalSnps += Integer.valueOf(fields[7]);
        mTransitions += Integer.valueOf(fields[8]);
        mTransversions += Integer.valueOf(fields[9]);
        mHeterozygousSnps += Integer.valueOf(fields[10]);
        mHomozygousSnps += Integer.valueOf(fields[11]);

        mTotalMnps += Integer.valueOf(fields[12]);
        mHeterozygousMnps += Integer.valueOf(fields[13]);
        mHomozygousMnps += Integer.valueOf(fields[14]);

        mTotalMixeds += Integer.valueOf(fields[15]);
        mHeterozygousMixeds += Integer.valueOf(fields[16]);
        mHomozygousMixeds += Integer.valueOf(fields[17]);

        mTotalInsertions += Integer.valueOf(fields[18]);
        mHeterozygousInsertions += Integer.valueOf(fields[19]);
        mHomozygousInsertions += Integer.valueOf(fields[20]);

        mTotalDeletions += Integer.valueOf(fields[21]);
        mHeterozygousDeletions += Integer.valueOf(fields[22]);
        mHomozygousDeletions += Integer.valueOf(fields[23]);

        mTotalBreakends += Integer.valueOf(fields[24]);
        mHeterozygousBreakends += Integer.valueOf(fields[25]);
        mHomozygousBreakends += Integer.valueOf(fields[26]);
    }

    public String toReducerString(){
        StringBuilder sb = new StringBuilder();
        sb.append(sampleName);
        sb.append("\t");
        String value = String.join("\t", getStatistics().getSecond());
        sb.append(value);
        return sb.toString();
    }

    public void add(VariantContext vc, String sample) {
        setSampleName(sample);
        Genotype gt = vc.getGenotype(sample);
        VariantType type = VariantType.determineType(vc, sample);
        if(gt.isNoCall()) {
            mNoCall++;
            return;
        }

        if (gt.isHet())
            mHeterozygous++;
        else
            mHomozygous++;

        switch (type) {
            case SNP:
                if (gt.isHet()) mHeterozygousSnps++;
                else mHomozygousSnps++;
                mTotalSnps++;
                for(Allele allele: gt.getAlleles()){
                    tallyTransitionTransversionRatio(vc.getReference().getBaseString(), allele.getBaseString());
                }
                break;
            case MNP:
                if (gt.isHet()) mHeterozygousMnps++;
                else mHomozygousMnps++;
                mTotalMnps++;
                break;
            case MIXED:
                if (gt.isHet()) mHeterozygousMixeds++;
                else mHomozygousMixeds++;
                mTotalMixeds++;
                break;
            case INS:
                if (gt.isHet()) mHeterozygousInsertions++;
                else mHomozygousInsertions++;
                mTotalInsertions++;
                break;
            case DEL:
                if (gt.isHet()) mHeterozygousDeletions++;
                else mHomozygousDeletions++;
                mTotalDeletions++;
                break;
            case BND:
                if (gt.isHet()) mHeterozygousBreakends++;
                else mHomozygousBreakends++;
                mTotalBreakends++;
                break;
            default:
                break;
        }
    }

    Pair<List<String>, List<String>> getStatistics() {
        final List<String> names = new ArrayList<>();
        final List<String> values = new ArrayList<>();

        names.add("Same as reference");
        values.add(Long.toString(mTotalUnchanged));
        names.add("Total Het");
        values.add(Long.toString(mHeterozygous));
        names.add("Total Hom");
        values.add(Long.toString(mHomozygous));
        names.add("NoCall");
        values.add(Long.toString(mHomozygous));
        names.add("De Novo Genotypes");
        values.add(Long.toString(mDeNovo));
        names.add("Phased Genotypes");
        values.add(Long.toString(mPhased));

        names.add("SNPs");
        values.add(Long.toString(mTotalSnps));
        names.add("SNP Transitions");
        values.add(Long.toString(mTransitions));
        names.add("SNP Transversions");
        values.add(Long.toString(mTransversions));
        names.add("SNP Het");
        values.add(Long.toString(mHeterozygousSnps));
        names.add("SNP Hom");
        values.add(Long.toString(mHomozygousSnps));

        names.add("MNPs");
        values.add(Long.toString(mTotalMnps));
        names.add("MNP Het");
        values.add(Long.toString(mHeterozygousMnps));
        names.add("MNP Hom");
        values.add(Long.toString(mHomozygousMnps));

        names.add("MIXEDs");
        values.add(Long.toString(mTotalMixeds));
        names.add("MIXED Het");
        values.add(Long.toString(mHeterozygousMixeds));
        names.add("MIXED Hom");
        values.add(Long.toString(mHomozygousMixeds));

        names.add("Insertions");
        values.add(Long.toString(mTotalInsertions));
        names.add("Insertion Het");
        values.add(Long.toString(mHeterozygousInsertions));
        names.add("Insertion Hom");
        values.add(Long.toString(mHomozygousInsertions));

        names.add("Deletions");
        values.add(Long.toString(mTotalDeletions));
        names.add("Deletion Het");
        values.add(Long.toString(mHeterozygousDeletions));
        names.add("Deletion Hom");
        values.add(Long.toString(mHomozygousDeletions));

        names.add("Structural variant breakends");
        values.add(Long.toString(mTotalBreakends));
        names.add("Breakend Het");
        values.add(Long.toString(mHeterozygousBreakends));
        names.add("Breakend Hom");
        values.add(Long.toString(mHomozygousBreakends));

        return Pair.create(names, values);
    }

    Pair<List<String>, List<String>> getReportResult() {
        final List<String> names = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        names.add("SNPs");
        values.add(Long.toString(mTotalSnps));
        names.add("MNPs");
        values.add(Long.toString(mTotalMnps));
        names.add("Insertions");
        values.add(Long.toString(mTotalInsertions));
        names.add("Deletions");
        values.add(Long.toString(mTotalDeletions));
        names.add("Structural variant breakends");
        values.add(mTotalBreakends > 0 ? Long.toString(mTotalBreakends) : null);
        names.add("Same as reference");
        values.add(Long.toString(mTotalUnchanged));
        names.add("De Novo Genotypes");
        values.add(mDeNovo > 0 ? Long.toString(mDeNovo) : null);
        names.add("Phased Genotypes");
        final long totalNonMissingGenotypes = mTotalSnps + mTotalMnps + mTotalInsertions + mTotalDeletions + mTotalUnchanged;
        values.add(mPhased > 0 ? StatsUtils.percent(mPhased, totalNonMissingGenotypes) : null);
        names.add("SNP Transitions/Transversions");
        values.add(StatsUtils.divide(mTransitions, mTransversions));

        names.add("Total Het/Hom ratio");
        values.add(StatsUtils.divide(mHeterozygous, mHomozygous));
        names.add("SNP Het/Hom ratio");
        values.add(StatsUtils.divide(mHeterozygousSnps, mHomozygousSnps));
        names.add("MNP Het/Hom ratio");
        values.add(StatsUtils.divide(mHeterozygousMnps, mHomozygousMnps));
        names.add("Insertion Het/Hom ratio");
        values.add(StatsUtils.divide(mHeterozygousInsertions, mHomozygousInsertions));
        names.add("Deletion Het/Hom ratio");
        values.add(StatsUtils.divide(mHeterozygousDeletions, mHomozygousDeletions));
        names.add("Breakend Het/Hom ratio");
        values.add(mTotalBreakends > 0 ? StatsUtils.divide(mHeterozygousBreakends, mHomozygousBreakends) : null);

        names.add("Insertion/Deletion ratio");
        values.add(StatsUtils.divide(mTotalInsertions, mTotalDeletions));
        names.add("Indel/SNP+MNP ratio");
        values.add(StatsUtils.divide(mTotalInsertions + mTotalDeletions, mTotalSnps + mTotalMnps));
        return Pair.create(names, values);
    }

    private void tallyTransitionTransversionRatio(String ref, String pred) {
        final boolean transition = "AG".contains(ref) && "AG".contains(pred) || "CT".contains(ref) && "CT".contains(pred);
        if (transition) {
            mTransitions++;
        } else {
            mTransversions++;
        }
    }

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public String getReport() {
        Pair<List<String>, List<String>> pair = getReportResult();
        List<String> names = pair.getFirst();
        List<String> values = pair.getSecond();
        StringBuilder outString = new StringBuilder();

        for (int i = 0; i < names.size(); i++) {
            outString.append(names.get(i));
            outString.append(" :\t");
            outString.append(values.get(i));
            outString.append("\n");
        }

        return outString.toString();
    }
}
