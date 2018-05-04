/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
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

    private static final String[] VARIANT_TYPE_COUNT_LENGTH = {
            "SNP", "MNP", "INS", "DEL"
    };

    private static final String ALLELE_LENGTH_TAG = "AlleleLen:";

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

    protected long mTotalFailedFilters = 0;
    protected long mTotalPassedFilters = 0;

    protected long mTotalDbSnp = 0;

    protected final Histogram[] mAlleleLengths;


    public PerSampleVCFReport(){
        sampleName = null;
        mAlleleLengths = new Histogram[VARIANT_TYPE_COUNT_LENGTH.length];
        for (int i = VariantType.SNP.ordinal(); i < mAlleleLengths.length; ++i) {
            // i from SNP as we don't care about NO_CALL/UNCHANGED
            mAlleleLengths[i] = new Histogram();
        }
    }

    public void parseReducerString(String reducerStr){

        if(reducerStr.startsWith(ALLELE_LENGTH_TAG)){
            String[] fields = reducerStr.split("\t", 3);
            VariantType type = VariantType.valueOf(fields[1]);
            if(type.ordinal() < mAlleleLengths.length)
                mAlleleLengths[type.ordinal()].addHistogram(fields[2]);
        }

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

        mTotalFailedFilters += Integer.valueOf(fields[27]);
        mTotalPassedFilters += Integer.valueOf(fields[28]);

        mTotalDbSnp += Integer.valueOf(fields[29]);
    }

    public String toReducerString(){
        StringBuilder sb = new StringBuilder();
        sb.append(sampleName);
        sb.append("\t");
        String value = String.join("\t", getStatistics());
        sb.append(value);
        sb.append("\n");


        for (int i = VariantType.SNP.ordinal(); i < mAlleleLengths.length; ++i) {
            Histogram histogram = mAlleleLengths[i];
            sb.append(ALLELE_LENGTH_TAG);
            sb.append("\t");
            sb.append(VARIANT_TYPE_COUNT_LENGTH[i]);
            sb.append("\t");
            sb.append(histogram.toString());
            sb.append("\n");
        }

        return sb.toString();
    }

    public void add(VariantContext vc, String sample) {
        setSampleName(sample);
        Genotype gt = vc.getGenotype(sample);
        VariantType type = VariantType.determineType(vc, sample);

        if(vc.isFiltered())
            mTotalFailedFilters++;
        else
            mTotalPassedFilters++;

        if(gt.isNoCall()) {
            mNoCall++;
            return;
        }

        if(gt.isHomRef())
            mTotalUnchanged++;

        if (gt.isHet())
            mHeterozygous++;
        else
            mHomozygous++;

        if(vc.hasID())
            mTotalDbSnp++;

        for(Allele allele: gt.getAlleles()){
            if(allele.isReference())
                continue;
            tallyAlleleLengths(vc.getReference(), allele);
        }

        switch (type) {
            case SNP:
                if (gt.isHet()) mHeterozygousSnps++;
                else mHomozygousSnps++;
                mTotalSnps++;
                for(Allele allele: gt.getAlleles()){
                    if(allele.isReference())
                        continue;
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

    private void tallyAlleleLengths(Allele ref, Allele alt) {
        VariantType type = VariantType.typeOfBiallelicVariant(ref, alt);
        int len = Math.max(ref.length(), alt.length());
        if(type.ordinal() < mAlleleLengths.length)
            mAlleleLengths[type.ordinal()].increment(len, 1);
    }

    List<String> getStatistics() {
        final List<String> values = new ArrayList<>();

        values.add(Long.toString(mTotalUnchanged));
        values.add(Long.toString(mHeterozygous));
        values.add(Long.toString(mHomozygous));
        values.add(Long.toString(mHomozygous));
        values.add(Long.toString(mDeNovo));
        values.add(Long.toString(mPhased));

        values.add(Long.toString(mTotalSnps));
        values.add(Long.toString(mTransitions));
        values.add(Long.toString(mTransversions));
        values.add(Long.toString(mHeterozygousSnps));
        values.add(Long.toString(mHomozygousSnps));

        values.add(Long.toString(mTotalMnps));
        values.add(Long.toString(mHeterozygousMnps));
        values.add(Long.toString(mHomozygousMnps));

        values.add(Long.toString(mTotalMixeds));
        values.add(Long.toString(mHeterozygousMixeds));
        values.add(Long.toString(mHomozygousMixeds));

        values.add(Long.toString(mTotalInsertions));
        values.add(Long.toString(mHeterozygousInsertions));
        values.add(Long.toString(mHomozygousInsertions));

        values.add(Long.toString(mTotalDeletions));
        values.add(Long.toString(mHeterozygousDeletions));
        values.add(Long.toString(mHomozygousDeletions));

        values.add(Long.toString(mTotalBreakends));
        values.add(Long.toString(mHeterozygousBreakends));
        values.add(Long.toString(mHomozygousBreakends));

        values.add(Long.toString(mTotalFailedFilters));
        values.add(Long.toString(mTotalPassedFilters));

        values.add(Long.toString(mTotalDbSnp));

        return values;
    }

    Pair<List<String>, List<String>> getReportResult() {
        final List<String> names = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        names.add("SampleName");
        values.add(sampleName);
        names.add("Failed Filters");
        values.add(Long.toString(mTotalFailedFilters));
        names.add("Passed Filters");
        values.add(Long.toString(mTotalPassedFilters));
        names.add("NoCalls");
        values.add(Long.toString(mNoCall));
        names.add("SNPs");
        values.add(Long.toString(mTotalSnps));
        names.add("MNPs");
        values.add(Long.toString(mTotalMnps));
        names.add("MIXEDs");
        values.add(Long.toString(mTotalMixeds));
        names.add("Insertions");
        values.add(Long.toString(mTotalInsertions));
        names.add("Deletions");
        values.add(Long.toString(mTotalDeletions));
        names.add("Structural variant breakends");
        values.add(mTotalBreakends > 0 ? Long.toString(mTotalBreakends) : "-");
        names.add("Same as reference");
        values.add(Long.toString(mTotalUnchanged));
//        names.add("De Novo Genotypes");
//        values.add(mDeNovo > 0 ? Long.toString(mDeNovo) : null);
//        names.add("Phased Genotypes");
//        final long totalNonMissingGenotypes = mTotalSnps + mTotalMnps + mTotalInsertions + mTotalDeletions + mTotalUnchanged;
//        values.add(mPhased > 0 ? StatsUtils.percent(mPhased, totalNonMissingGenotypes) : null);
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
        values.add(mTotalBreakends > 0 ? StatsUtils.divide(mHeterozygousBreakends, mHomozygousBreakends) : "-");

        names.add("Insertion/Deletion ratio");
        values.add(StatsUtils.divide(mTotalInsertions, mTotalDeletions));
        names.add("Indel/SNP+MNP ratio");
        values.add(StatsUtils.divide(mTotalInsertions + mTotalDeletions, mTotalSnps + mTotalMnps));
        names.add("dbSNP ratio");
        values.add(StatsUtils.divide(mTotalDbSnp, mTotalPassedFilters - mNoCall));
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

    /**
     * Append per sample histograms to a buffer.
     * @param sb buffer to append to
     */
    public void appendHistograms(StringBuilder sb) {
        sb.append("Variant Allele Lengths :").append("\n");
        //sb.append("bin\tSNP\tMNP\tInsert\tDelete\tIndel").append(StringUtils.LS);
        sb.append("length");
        for (int i = VariantType.SNP.ordinal(); i < mAlleleLengths.length; ++i) {
            if (i <= VariantType.DEL.ordinal() || mAlleleLengths[i].getLength() != 0) {
                sb.append("\t").append(VARIANT_TYPE_COUNT_LENGTH[i]);
            }
        }
        sb.append("\n");

        int size = 0;
        final int[] lengths = new int[mAlleleLengths.length];
        for (int i = VariantType.SNP.ordinal(); i < mAlleleLengths.length; ++i) {
            lengths[i] = mAlleleLengths[i].getLength();
            if (lengths[i] > size) {
                size = lengths[i];
            }
        }
        int bin = 1;
        int step = 1;
        while (bin < size) {
            sb.append(bin);
            final int end = bin + step;
            if (end - bin > 1) {
                sb.append("-").append(end - 1);
            }
            for (int i = VariantType.SNP.ordinal(); i < mAlleleLengths.length; ++i) {
                if (i <= VariantType.DEL.ordinal() || mAlleleLengths[i].getLength() != 0) {
                    long sum = 0L;
                    for (int j = bin; j < end; ++j) {
                        if (j < lengths[i]) {
                            sum += mAlleleLengths[i].getValue(j);
                        }
                    }
                    sb.append("\t").append(sum);
                }
            }
            sb.append("\n");
        }
    }

    public String getReport() {
        Pair<List<String>, List<String>> pair = getReportResult();
        List<String> names = pair.getFirst();
        List<String> values = pair.getSecond();
        StringBuilder outString = new StringBuilder();

        for (int i = 0; i < names.size(); i++) {
            outString.append(String.format("%-37s", names.get(i)));
            outString.append(":  ");
            outString.append(values.get(i));
            outString.append("\n");
        }

        appendHistograms(outString);

        return outString.toString();
    }
}
