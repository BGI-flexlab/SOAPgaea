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

    private Map<String, PerSampleVCFReport> PerSampleVCFReports;

    public VCFReport(){
        PerSampleVCFReports = new HashMap<>();
    }

    public void parseVariation(VariantContext vc){
        PerSampleVCFReport PerSampleVCFReport;
        for(String sample: vc.getSampleNames()){
            Genotype gt = vc.getGenotype(sample);
            if(!gt.isCalled())
                return;

            if(PerSampleVCFReports.containsKey(sample))
                PerSampleVCFReport = PerSampleVCFReports.get(sample);
            else {
                PerSampleVCFReport = new PerSampleVCFReport();
                PerSampleVCFReports.put(sample, PerSampleVCFReport);
            }

            PerSampleVCFReport.add(vc, sample);
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
        PerSampleVCFReport PerSampleVCFReport;
        while ((lineReader.readLine(line)) != 0) {
            String sample = line.toString().split("\t")[0];
            if(PerSampleVCFReports.containsKey(sample))
                PerSampleVCFReport = PerSampleVCFReports.get(sample);
            else {
                PerSampleVCFReport = new PerSampleVCFReport();
                PerSampleVCFReports.put(sample, PerSampleVCFReport);
            }
            PerSampleVCFReport.parseReducerString(line.toString());
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
        FileStatus filelist[] = fs.listStatus(input);
//        FileStatus filelist[] = fs.listStatus(input,new StaticPathFilter());

        for (int i = 0; i < filelist.length; i++) {
            if (!filelist[i].isDirectory()) {
                readFromHdfs(filelist[i].getPath(), conf);
                fs.delete(filelist[i].getPath(), false);
            }
        }

        fs.close();
        for(String sample: getPerSampleVCFReports().keySet()){
            PerSampleVCFReport PerSampleVCFReport = getPerSampleVCFReports().get(sample);
            String fileName = outputDir + "/" + sample + ".vcfstats.report.txt";
            write(fileName, conf, PerSampleVCFReport.getReport());
        }

    }

    public Map<String, PerSampleVCFReport> getPerSampleVCFReports() {
        return PerSampleVCFReports;
    }

    static class StaticPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            if (path.getName().startsWith("Statistic"))
                return true;
            return false;
        }
    }
}
