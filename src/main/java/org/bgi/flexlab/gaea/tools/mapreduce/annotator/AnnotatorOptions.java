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
package org.bgi.flexlab.gaea.tools.mapreduce.annotator;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.seqdoop.hadoop_bam.SAMFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by huangzhibo on 2017/7/7.
 */
public class AnnotatorOptions extends GaeaOptions implements HadoopOptions{
    private final static String SOFTWARE_NAME = "Annotator";
    private final static String SOFTWARE_VERSION = "1.0";

    private String input;
    private ArrayList<Path> inputFileList;
    private int inputFormat;
    private String output;
    private int outputFormat;
    private boolean outputDupRead;
    private boolean isSE;
    private int reducerNum;
    private int windowSize;
    private int extendSize;
    FileSystem fs;

    public AnnotatorOptions() {
        addOption("i", "input",      true,  "input file(VCF). [request]", true);
        addOption("o", "output",     true,  "output file of annotation results(.gz) [request]", true);
        addOption("c", "config",     true,  "config file. [request]", true);
        addOption("r", "reference",  true,  "indexed reference sequence file list [request]", true);
        //addOption("T", "outputType", true,  "output file foramt[txt, vcf].");
        addOption("m", "mapperNum", true,  "mapper number. [50]");
        addOption(null,"cacheref",   false,  "DistributedCache reference sequence file list");
        addOption(null,"verbose",    false, "display verbose information.");
        addOption(null,"debug",      false, "for debug.");
        addOption("h", "help",       false, "help information.");
        FormatHelpInfo(SOFTWARE_NAME,SOFTWARE_VERSION);

        inputFileList = new ArrayList<>();
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

        input = getOptionValue("i", null);
        inputFormat = getOptionIntValue("I", 1);
        output = getOptionValue("o", null);
        outputFormat = getOptionIntValue("O", 0);
        outputDupRead = getOptionBooleanValue("D", true);
        isSE = getOptionBooleanValue("S", false);
        reducerNum = getOptionIntValue("R", 30);
        windowSize = getOptionIntValue("W", 100000);
        extendSize = getOptionIntValue("E", 100);
    }

    @Override
    public void setHadoopConf(String[] args, Configuration conf) {
        conf.setStrings("args", args);
        Path p = new Path(this.getInput());
        try {
            fs = p.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        traversalInputPath(this.getInput());
    }

    @Override
    public void getOptionsFromHadoopConf(Configuration conf) {
        String[] args = conf.getStrings("args");
        this.parse(args);
    }

    private void traversalInputPath(String input)
    {
        Path path = new Path(input);
        try {
            if (!fs.exists(path)) {
                System.err.println("Input File Path is not exist! Please check -i var.");
                System.exit(-1);
            }
            if (fs.isFile(path)) {
                inputFileList.add(path);
            }else {
                FileStatus stats[]=fs.listStatus(path);

                for (FileStatus file : stats) {
                    Path filePath=file.getPath();

                    if (!fs.isFile(filePath)) {
                        String childPath=filePath.toString();
                        traversalInputPath(childPath);
                    }else {
                        inputFileList.add(filePath);
                    }
                }
            }
        }catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public ArrayList<Path> getInputFileList(){
        return inputFileList;
    }

    public String getInput() {
        return input;
    }

    public SAMFormat getInputFormat(){
        return inputFormat == 0 ? SAMFormat.BAM :SAMFormat.SAM;
    }

    public String getOutput() {
        return output;
    }

    public int getOutputFormat() {
        return outputFormat;
    }

    public boolean isOutputDupRead() {
        return outputDupRead;
    }

    public boolean isSE() {
        return isSE;
    }

    public int getReducerNum() {
        return reducerNum;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public int getExtendSize() {
        return extendSize;
    }
}
