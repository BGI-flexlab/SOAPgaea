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
package org.bgi.flexlab.gaea.tools.mapreduce.bamsort;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.seqdoop.hadoop_bam.SAMFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class BamSortOptions extends GaeaOptions implements HadoopOptions {

    private final static String SOFTWARE_NAME = "BamSort";
    private final static String SOFTWARE_VERSION = "1.0";

    private List<String> strInputs = null;
    private HashMap<String,String> renames = null;
    private String outdir = null;
    private String tmpPath = null;
    private String reference = null;
    private String inputFormat = "BAM";
    private String outputFormat = "BAM";
    private String type = "all";
    private boolean isMultiSample = true;
    private boolean verbose = false;

    private int reducerNum;

    public BamSortOptions() {
        addOption("i", "input",      true,  "input file list. [request]", true);
        addOption("I", "inputFormat",      true,  "input format SAM/BAM. [BAM]");
        addOption("o", "outdir",     true,  "output dir [request]", true);
        addOption("O", "outputFormat", true,  "output file foramt SAM/BAM [BAM].");
        addOption("m", "multiSample", false,  "have more than one sample in input file list");
        addOption("n", "rename", true,  "replace sample name list [null]");
        addOption("r", "reference", true,  "reference [null]");
        addOption("h", "help",       false, "help information.");
        addOption("R", "reducer", true, "reducer numbers [30]");
        addOption("T","type",    true, "filter mode. unmap/all [all]");
//        addOption(null,"tmpdir",    true, "hdfs tmpdir [default]");
        addOption(null,"verbose",    false, "display verbose information.");

        FormatHelpInfo(SOFTWARE_NAME,SOFTWARE_VERSION);

    }

    @Override
    public void parse(String[] args) {
        try {
            cmdLine = parser.parse(options, args);
            if(cmdLine.hasOption("h")) {
                helpInfo.printHelp("Options:", options, true);
                System.exit(1);
            }
        } catch (ParseException e) {
            helpInfo.printHelp("Options:", options, true);
            System.exit(1);
        }


        reducerNum = getOptionIntValue("R", 30);

//        if(cmdLine.hasOption("tmpdir")){
//            setTmpPath(cmdLine.getOptionValue("tmpdir"));
//        }else {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            tmpPath = cmdLine.getOptionValue("outdir") + "/bamsort-tmp-" + df.format(new Date());
//        }

        try {
            setStrInputs(cmdLine.getOptionValue("input"));
            setRenames(getOptionValue("rename",null));
        } catch (IOException e) {
            e.printStackTrace();
        }
        setOutputFormat(getOptionValue("outputFormat", "BAM"));
        setInputFormat(getOptionValue("inputFormat","BAM"));
        setOutdir(cmdLine.getOptionValue("outdir"));
        setMultiSample(getOptionBooleanValue("multiSample", false));
        setVerbose(getOptionBooleanValue("verbose", false));
        setType(getOptionValue("type", "all"));
        setReference(getOptionValue("reference",null));
        setReducerNum(getOptionIntValue("reducer",30));
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

    public int getReducerNum() {
        return reducerNum;
    }

    public void setReducerNum(int reducerNum) {
        this.reducerNum = reducerNum;
    }

    private SAMFormat getFormat(String f){
        SAMFormat format = null;
        switch (f) {
            case "SAM":
                format = SAMFormat.SAM;
                break;
            case "BAM":
                format = SAMFormat.BAM;
                break;
        }
        return format;
    }

    public SAMFormat getInputFormat() {
        return getFormat(inputFormat);
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public SAMFormat getOutputFormat(Configuration conf) {
        SAMFormat format = null;
        switch (outputFormat) {
            case "CRAM":
                conf.set("reference", reference);
                break;
            case "SAM":
                format = SAMFormat.SAM;
                break;
            case "BAM":
                format = SAMFormat.BAM;
                break;
        }
        return format;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public List<String> getStrInputs() {
        return strInputs;
    }

    public void setStrInputs(String inputList) throws IOException {
        List<String> strInputs = new ArrayList<>();

        FileReader fr = new FileReader(inputList);
        BufferedReader br = new BufferedReader(fr);

        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if(line.isEmpty())
                continue;
            strInputs.add(line);
        }

        br.close();
        fr.close();
        this.strInputs = strInputs;
    }

    public boolean isMultiSample() {
        return isMultiSample;
    }

    public void setMultiSample(boolean multiSample) {
        isMultiSample = multiSample;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getOutdir() {
        return outdir;
    }

    public void setOutdir(String outdir) {
        this.outdir = outdir;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setRenames(String renameFile) throws IOException {
        if(renameFile == null)
            return;
        HashMap<String,String> replaceList = new HashMap<>();

        FileReader fr = new FileReader(renameFile);
        BufferedReader br = new BufferedReader(fr);

        String line;
        while ((line = br.readLine()) != null) {
            String[] sampleNames = line.trim().split("\t");
            replaceList.put(sampleNames[0].trim(), sampleNames[1].trim());
        }

        br.close();
        fr.close();
        this.renames = replaceList;
    }

    public HashMap<String,String> getRenames() {
        return renames;
    }

    public void setTmpPath(String tmpdir) {
        tmpPath = tmpdir;
    }

    public String getTmpPath() {
        return tmpPath;
    }
}
