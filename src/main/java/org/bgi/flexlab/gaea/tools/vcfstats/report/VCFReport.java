package org.bgi.flexlab.gaea.tools.vcfstats.report;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VCFReport {

    private Map<String, SampleVCFReport> sampleVCFReports;

    public VCFReport(){
        sampleVCFReports = new HashMap<>();
    }

    public void parseVariation(VariantContext vc){
        SampleVCFReport sampleVCFReport;
        for(String sample: vc.getSampleNames()){
            Genotype gt = vc.getGenotype(sample);
            if(!gt.isCalled())
                return;

            if(sampleVCFReports.containsKey(sample))
                sampleVCFReport = sampleVCFReports.get(sample);
            else {
                sampleVCFReport = new SampleVCFReport();
                sampleVCFReports.put(sample, sampleVCFReport);
            }

            sampleVCFReport.add(vc, sample);
        }
    }

    public String parseReducerString(){
        return null;
    }

    public String toReducerString(){
        return null;
    }

    public void readFromHdfs(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream FSinput = fs.open(path);

        LineReader lineReader = new LineReader(FSinput, conf);
        Text line = new Text();
        SampleVCFReport sampleVCFReport;
        while ((lineReader.readLine(line)) != 0) {
            String sample = line.toString().split("\t")[0];
            if(sampleVCFReports.containsKey(sample))
                sampleVCFReport = sampleVCFReports.get(sample);
            else {
                sampleVCFReport = new SampleVCFReport();
                sampleVCFReports.put(sample, sampleVCFReport);
            }
            sampleVCFReport.parseReducerString(line.toString());
        }
        lineReader.close();
    }

    private void write(String fileName, Configuration conf, String report)
            throws IOException {
        Path reportFilePath = new Path(fileName);
        FileSystem fs = reportFilePath.getFileSystem(conf);
        FSDataOutputStream writer = fs.create(reportFilePath);
        writer.write(report.getBytes());
        writer.close();
    }

    public void mergeReport(Path input, Configuration conf, Path outputDir) throws IOException {
        FileSystem fs = input.getFileSystem(conf);
        FileStatus filelist[] = fs.listStatus(input,new StaticPathFilter());

        for (int i = 0; i < filelist.length; i++) {
            if (!filelist[i].isDirectory()) {
                readFromHdfs(filelist[i].getPath(), conf);
                fs.delete(filelist[i].getPath(), false);
            }
        }

        fs.close();
        for(String sample: getSampleVCFReports().keySet()){
            SampleVCFReport sampleVCFReport = getSampleVCFReports().get(sample);
            String fileName = outputDir + "/" + sample + ".vcfstats.report.txt";
            write(fileName, conf, sampleVCFReport.getReport());
        }

    }

    public Map<String, SampleVCFReport> getSampleVCFReports() {
        return sampleVCFReports;
    }

    static class StaticPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            if (path.getName().startsWith("filterStatistic"))
                return true;
            return false;
        }
    }
}
