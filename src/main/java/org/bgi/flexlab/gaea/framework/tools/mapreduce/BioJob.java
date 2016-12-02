package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.input.bam.GaeaAnySAMInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.partitioner.WindowsBasedBasicSort;
import org.bgi.flexlab.gaea.data.mapreduce.partitioner.WindowsBasedComparator;
import org.bgi.flexlab.gaea.data.mapreduce.partitioner.WindowsBasedPartitioner;
import org.bgi.flexlab.gaea.data.mapreduce.partitioner.WindowsBasedSort;
import org.bgi.flexlab.gaea.data.structure.bam.filter.SamRecordFilter;
import org.seqdoop.hadoop_bam.SAMFormat;

public class BioJob extends Job {

	@SuppressWarnings("deprecation")
	private BioJob(Configuration conf) throws IOException {
		super(conf);
	}

	public static BioJob getInstance() throws IOException {
		return getInstance(new Configuration());
	}

	public static BioJob getInstance(Configuration conf) throws IOException {
		return new BioJob(conf);
	}

	@SuppressWarnings("rawtypes")
	public void setWindowsBasicMapperClass(Class<? extends Mapper> cls,
			int windowsSize) throws IllegalStateException {
		conf.setInt(WindowsBasedMapper.WINDOWS_SIZE, windowsSize);
		setMapperClass(cls);
		setPartitionerClass(WindowsBasedPartitioner.class);
		setGroupingComparatorClass(WindowsBasedComparator.class);
		setSortComparatorClass(WindowsBasedSort.class);
		//setSortComparatorClass(WindowsBasedBasicSort.class);
	}

	@SuppressWarnings("rawtypes")
	public void setWindowsBasicMapperClass(Class<? extends Mapper> cls,
			int windowsSize, int windowsExtendSize)
			throws IllegalStateException {
		conf.setInt(WindowsBasedMapper.WINDOWS_EXTEND_SIZE, windowsExtendSize);
		setWindowsBasicMapperClass(cls, windowsSize);
	}
	
	public void setWindowsBasicMapperClass(@SuppressWarnings("rawtypes") Class<? extends Mapper> cls,
			int windowsSize, int windowsExtendSize,boolean multiSample)
			throws IllegalStateException {
		if(multiSample)
			setMultipleSample();
		setWindowsBasicMapperClass(cls,windowsSize,windowsExtendSize);
	}

	public void setMultipleSample() {
		conf.setBoolean(WindowsBasedMapper.MULTIPLE_SAMPLE, true);
	}

	/*
	 * set windows based mapper filter
	 * */
	public void setFilterClass(Class<? extends SamRecordFilter> cls) {
		conf.setClass(WindowsBasedMapper.SAM_RECORD_FILTER, cls,
				SamRecordFilter.class);
	}

	public void setOutputKeyValue(Class<?> mapKeyClass, Class<?> mapValueClass,
			Class<?> reduceKeyClass, Class<?> reduceValueClass) {
		setMapOutputKeyClass(mapKeyClass);
		setMapOutputValueClass(mapValueClass);
		setOutputKeyClass(reduceKeyClass);
		setOutputValueClass(reduceValueClass);
	}

	public void setOutputKeyValue(Class<?> keyClass, Class<?> valueClass) {
		setOutputKeyClass(keyClass);
		setOutputValueClass(valueClass);
	}
	
	/*
	 * set sam or bam inputformat
	 * */
	public void setAnySamInputFormat(SAMFormat fmt){
		conf.set(GaeaAnySAMInputFormat.SAM_FORMAT_FOR_ALL_PATH, fmt.toString());
		setInputFormatClass(GaeaAnySAMInputFormat.class);
	}
	
	public void setHeader(Path input,Path output){
		try {
			SamHdfsFileHeader.loadHeader(input,conf,output);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}
}
