package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class BioJob extends Job {

	@SuppressWarnings("deprecation")
	public BioJob(Configuration conf) throws IOException {
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
		conf.setInt(WindowsBasicMapper.WINDOWS_SIZE, windowsSize);
		setMapperClass(cls);
	}

	@SuppressWarnings("rawtypes")
	public void setWindowsBasicMapperClass(Class<? extends Mapper> cls,
			int windowsSize, int windowsExtendSize)
			throws IllegalStateException {
		conf.setInt(WindowsBasicMapper.WINDOWS_EXTEND_SIZE, windowsExtendSize);
		setWindowsBasicMapperClass(cls,windowsSize);
	}
	
	public void setMultipleSample(){
		conf.setBoolean(WindowsBasicMapper.MULTIPLE_SAMPLE, true);
	}
}
