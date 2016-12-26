package org.bgi.flexlab.gaea.tools.mapreduce.genotyper;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

/**
 * Created by zhangyong on 2016/12/20.
 */
public class GenotyperOptions extends GaeaOptions implements HadoopOptions {
    private final static String SOFTWARE_NAME = "Genotyper";
    private final static String SOFTWARE_VERSION = "1.0";



    public GenotyperOptions() {

    }

    @Override
    public void parse(String[] args) {

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
}
