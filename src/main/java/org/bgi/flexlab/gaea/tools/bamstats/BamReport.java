package org.bgi.flexlab.gaea.tools.bamstats;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.tools.mapreduce.bamstats.BamStatsOptions;
import org.bgi.flexlab.gaea.util.Histogram;

import java.io.IOException;

public class BamReport {

//    private String sampleName;
    private long mTotalReads = 0;
    private long mTotalReadsExcDup = 0;

    private Histogram readsNumByMapLength = null;


    public BamReport(BamStatsOptions options) {
        readsNumByMapLength = new Histogram();
    }

    public void parseReducerString(String reducerStr){
        readsNumByMapLength.addHistogram(reducerStr);
    }

    public String toReducerString(){
        return readsNumByMapLength.toString() + "\n";
    }

    public void add(SAMRecord samRecord) {
        if(samRecord.getReadUnmappedFlag()) {
            readsNumByMapLength.increment(0);
            return;
        }

        int map_len = 0;
        for (CigarElement cigarElement: samRecord.getCigar().getCigarElements()) {
            if(cigarElement.getOperator().isAlignment())
                map_len += cigarElement.getLength();
        }
        readsNumByMapLength.increment(map_len);
    }


    public void mergeReport(Path input, Configuration conf, Path outputDir) throws IOException {
        FileSystem fs = input.getFileSystem(conf);
        FileStatus[] filelist = fs.listStatus(input);

        for (FileStatus aFilelist : filelist) {
            if (!aFilelist.isDirectory()) {
                readFromHdfs(aFilelist.getPath(), conf);
            }
        }
        fs.close();
        String fileName = outputDir + "/bamstats.mapLengthCount.txt";
        write(fileName, conf, getReport());
    }

    private void write(String fileName, Configuration conf, String report)
            throws IOException {
        Path reportFilePath = new Path(fileName);
        FileSystem fs = reportFilePath.getFileSystem(conf);
        FSDataOutputStream writer = fs.create(reportFilePath);
        writer.write(report.getBytes());
        writer.close();
    }


    public void readFromHdfs(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream FSinput = fs.open(path);

        LineReader lineReader = new LineReader(FSinput, conf);
        Text line = new Text();
        while ((lineReader.readLine(line)) != 0) {
            String reducerStr = line.toString();
            if(reducerStr.isEmpty()) continue;
            parseReducerString(reducerStr);
        }
        lineReader.close();
    }

    public String getReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("#Map Length Count:").append("\n");
        sb.append("#Length\tReads_Number\n");

        for (int i = 0; i < readsNumByMapLength.getLength(); ++i) {
            sb.append(i);
            sb.append("\t");
            sb.append(readsNumByMapLength.getValue(i));
            sb.append("\n");
        }
        return sb.toString();
    }
}
