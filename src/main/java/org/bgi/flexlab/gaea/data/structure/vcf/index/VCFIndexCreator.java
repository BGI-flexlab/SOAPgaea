package org.bgi.flexlab.gaea.data.structure.vcf.index;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.util.HdfsFileManager;



public class VCFIndexCreator implements IndexCreator {

	private final int defaultBlockSize = 10000;
	private String VCFFile = null;
	private int blockSize;
	private String idxPath;
	private Configuration conf = new Configuration();
	@Override
	public void initialize(String inputFile, int binSize) {
		// TODO Auto-generated method stub
		this.VCFFile=inputFile;
		this.blockSize=binSize;
		idxPath = vcfIndexPath();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Index finalizeIndex() throws IOException {
		// TODO Auto-generated method stub
		if(VCFFile==null||VCFFile.equals("")) {
			return null;
		}
		VCFIndex idx=new VCFIndex();
		idx.initialize(conf, blockSize);
		FileSystem fs = HdfsFileManager.getFileSystem(new Path(VCFFile), conf);
		Path idxpath=new Path(idxPath);
		if(fs.exists(idxpath)) {
			FSDataInputStream fsInputStream = fs.open(idxpath);
			idx.read(fsInputStream);
			fs.close();
		} else {
			Path vcfpath = new Path(VCFFile);
			FSDataInputStream fsInputStream = fs.open(vcfpath);
			idx.create(fsInputStream);
			fsInputStream.close();
			FSDataOutputStream fsOutputStream = fs.create(idxpath);
			idx.write(fsOutputStream);
			fsOutputStream.close();
		}
			
		return idx;
	}
	
	public void deleteIndex() throws IOException {
		FileSystem fs = HdfsFileManager.getFileSystem(new Path(VCFFile), conf);
		Path idxpath = new Path(idxPath);
		if(fs.exists(idxpath)) {
			fs.delete(idxpath, false);
		}
		fs.close();
	}

	@Override
	public int defaultBinSize() {
		// TODO Auto-generated method stub
		return defaultBlockSize;
	}

	@Override
	public int getBinSize() {
		// TODO Auto-generated method stub
		return blockSize;
	}
	
	private String vcfIndexPath()
	{
		return VCFFile+".gaeaidx";
	}
	
}
