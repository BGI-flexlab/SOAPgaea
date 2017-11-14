package org.bgi.flexlab.gaea.tools.mapreduce.annotator.databaseload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.datawarehouse.genomics.LoadVCFToHBaseOptions;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFMultipleInputFormat;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;

import java.io.IOException;

public class DBNSFPToHbase extends ToolsRunner {

    private LoadVCFToHBaseOptions options = null;
    private Connection conn = null;

    private void createTable(TableName tableName) throws IOException {
        Admin admin = conn.getAdmin();
        if (admin.tableExists(tableName)) {
            admin.close();
            return;
        }
        HTableDescriptor table = new HTableDescriptor(tableName);
        table.addFamily(new HColumnDescriptor("info"));
        admin.createTable(table);
        admin.close();
    }

    private void LoadHFile2HBase(Configuration conf, TableName tableName,String hfile) throws Exception{
        conf.set("hbase.metrics.showTableName", "false");
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(tableName);
        RegionLocator rl = conn.getRegionLocator(tableName);

        loader.doBulkLoad(new Path(hfile), admin, table, rl);
//        loader.doBulkLoad(new Path(hfile), table);
        table.close();
        admin.close();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        String[] remainArgs = remainArgs(args, conf);
        options = new LoadVCFToHBaseOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);

        conf.addResource(new Path(options.getConfig() + "hbase-site.xml"));
        conf.addResource(new Path(options.getConfig() + "core-site.xml"));
        Job job =  Job.getInstance(conf, "dbNSFPtoHbase");

        createTable(conf, TableName.valueOf(options.getTableName()));
        MultipleVCFHeader vcfHeaders = new MultipleVCFHeader();
        vcfHeaders.mergeHeader(new Path(options.getInput()),options.getHeaderOutput(), job, false);

        job.setNumReduceTasks(options.getReducerNumber());
        job.setInputFormatClass(VCFMultipleInputFormat.class);

        job.setJarByClass(org.bgi.flexlab.gaea.tools.mapreduce.annotator.databaseload.DBNSFPToHbase.class);
        job.setMapperClass(DBNSFPToHbaseMapper.class);
        job.setReducerClass(PutSortReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.setInputPaths(job, new Path(options.getInput()));
        FileOutputFormat.setOutputPath(job, new Path(options.getHFileOutput()));

        HFileOutputFormat2.configureIncrementalLoad(job, new HTable(conf,options.getTableName()));

        if (job.waitForCompletion(true)) {
            LoadHFile2HBase(conf,options.getTableName(),options.getHFileOutput());
            return 0;
        } else {
            return 1;
        }
    }

}
