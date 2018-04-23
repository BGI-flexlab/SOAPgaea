package org.bgi.flexlab.gaea.tools.vcfstats.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class VCFReport {

    private long snpNum;
    private long indelNum;
    private long Insertions;
    private long Deletions;
    private long MNPs;
    private long sameAsReference;
    private long titv;
    private long missingGenotype;
    private long dnSNPnum;


//    private long Total_Het_Hom_ratio;
//    private long SNP_Het_Hom_ratio;
//    private long MNP_Het_Hom_ratio;
//    private long Insertion_Het_Hom_ratio;
//    private long Deletion_Het_Hom_ratio;
//    private long Indel_Het_Hom_ratio;
//    private long Insertion_Deletion_ratio;
//    private long Indel_SNP_MNP_ratio;
//    private long dbSNPratio;


    public void mergeReport(Path partTmp, Configuration conf, Path path) {



    }



}
