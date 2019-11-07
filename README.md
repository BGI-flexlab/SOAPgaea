# SOAPgaeaDevelopment4.0
SOAPgaea Refactoring version

## how to run?
`
hadoop jar gaea-1.0.0.jar [tool_name] --help
`

## tools
hadoop jar gaea-1.0.0.jar --help
```
 tools list:
 Annotator
 BamQualityControl     Gaea bam quality control
                       The purpose of bam quality control is to attain statistics informationof the bam file
 BamSort               Gaea BamSort
 BamStats
 CallSV                Gaea structural variantion calling
 FastqMerge            Load Clean Fastq Data from HDFS
 FastqQualityControl   Gaea fastq quality control

 Genotyper             Gaea genotyper
 HaplotypeCaller
 JointCalling          joing calling for gvcfs
 JointcallingEval
 MarkDuplicate         Gaea Mark PCR duplication
 Realigner             Gaea realigner and base quality recalibrator!

 UploadCram            Upload cram to hdfs(in bam format)
 VCFStats
 VQSR                  Gaea vcf quality control!
 ``` 

## build reference index
java -cp gaea-1.0.0.jar org.bgi.flexlab.gaea.data.structure.reference.index.VcfIndex reference_path [dbsnp_path] [output_path]
