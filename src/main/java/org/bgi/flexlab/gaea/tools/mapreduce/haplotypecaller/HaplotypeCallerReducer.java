package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.input.bed.RegionHdfsParser;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.util.Window;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.samtools.SAMFileHeader;

public class HaplotypeCallerReducer extends Reducer<WindowsBasedWritable, SAMRecordWritable, NullWritable, VariantContextWritable>{

	/**
     * region
     */
    private RegionHdfsParser region = null;
    
    private HaplotypeCallerOptions options = new HaplotypeCallerOptions();
    
    /**
     * sam file header
     */
    private SAMFileHeader header;

    /**
     * shared reference
     */
    private ReferenceShare genomeShare;
    
	@Override
    protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);
		
		if(options.getRegion() != null){
			region = new RegionHdfsParser();
            region.parseBedFileFromHDFS(options.getRegion(), false);
		}
		
		header = SamHdfsFileHeader.getHeader(conf);
        genomeShare = new ReferenceShare();
        genomeShare.loadChromosomeList(options.getReference());
	}
	
	@Override
    public void reduce(WindowsBasedWritable key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
		Window win = new Window(header, key.getChromosomeIndex(), key.getWindowsNumber(), options.getWindowSize());
		
		ChromosomeInformationShare chrInfo = genomeShare.getChromosomeInfo(header.getSequence(key.getChromosomeIndex()).getSequenceName());
	}
	
	@Override
    protected void cleanup(Context context) {

    }
}
