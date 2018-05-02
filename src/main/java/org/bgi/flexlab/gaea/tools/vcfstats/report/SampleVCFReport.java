package org.bgi.flexlab.gaea.tools.vcfstats.report;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.structure.reads.report.FastqQualityControlReport;
import org.bgi.flexlab.gaea.data.structure.reads.report.FastqQualityControlReporterIO;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;

public class SampleVCFReport {

    private String sampleName;
    private long snpNum;
    private long indelNum;
    private long insertionNum;
    private long deletionNum;
    private long mnpNum;
    private long mixedNum;
    private long ti;
    private long tv;
    private long missingGenotype;
    private long dbSNPnum;
    private long snpHetNum;
    private long snpHomNum;


    public SampleVCFReport(){
        sampleName = null;
        snpNum = indelNum = insertionNum = deletionNum = mnpNum = 0;
        ti = tv = missingGenotype = dbSNPnum = 0;
    }

    public void parseReducerString(String reducerStr){
        String[] fields = reducerStr.split("\t");
        if(sampleName == null)
            sampleName = fields[0];
        snpNum += Integer.valueOf(fields[1]);
        indelNum += Integer.valueOf(fields[2]);
        insertionNum += Integer.valueOf(fields[3]);
        deletionNum += Integer.valueOf(fields[4]);
        mnpNum += Integer.valueOf(fields[5]);
        mixedNum += Integer.valueOf(fields[6]);
        ti += Integer.valueOf(fields[7]);
        tv += Integer.valueOf(fields[8]);
    }

    public String toReducerString(){
        StringBuilder sb = new StringBuilder();
        sb.append(sampleName);
        sb.append("\t");
        sb.append(snpNum);
        sb.append("\t");
        sb.append(indelNum);
        sb.append("\t");
        sb.append(insertionNum);
        sb.append("\t");
        sb.append(deletionNum);
        sb.append("\t");
        sb.append(mnpNum);
        sb.append("\t");
        sb.append(mixedNum);
        sb.append("\t");
        sb.append(ti);
        sb.append("\t");
        sb.append(tv);
        return sb.toString();
    }

    public void add(VariantContext vc, String sample) {
        setSampleName(sample);
        Genotype gt = vc.getGenotype(sample);
        if(vc.getType() == VariantContext.Type.SNP){
            snpNum ++ ;
            if(gt.isHet()) snpHetNum++;
            if(gt.isHomVar()) snpHomNum++;
        }else if(vc.getType() == VariantContext.Type.INDEL){
            indelNum ++;
            if(vc.isSimpleDeletion())
                deletionNum ++;
            else if(vc.isSimpleInsertion())
                insertionNum ++;
        }else if(vc.getType() == VariantContext.Type.MNP){
            mnpNum ++;
        }else if(vc.getType() == VariantContext.Type.MIXED)
            mixedNum ++;

        for(Allele allele : gt.getAlleles()){
            allele.isReference();

        }
    }

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public String getReport() {
        DecimalFormat df = new DecimalFormat("0.000");
        df.setRoundingMode(RoundingMode.HALF_UP);

        StringBuilder outString = new StringBuilder();
        outString.append("SNPs:\t");
        outString.append(snpNum);

//        todo
        return outString.toString();
    }
}
