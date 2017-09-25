package org.bgi.flexlab.gaea.tools.jointcalling.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFHdfsLoader;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.VCFHdfsWriter;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.variant.vcf.VCFHeader;

public class MultipleVCFHeaderForJointCalling extends GaeaVCFHeader implements Serializable {

	private static final long serialVersionUID = -3352974548797591555L;

	private HashMap<String, VCFHeader> headers = new HashMap<String, VCFHeader>();
	
	private String HEADER_DEFAULT_PATH = "vcfHeaders";

	public MultipleVCFHeaderForJointCalling() {
	}

	public VCFHeader getVCFHeader(String sampleName) {
		if (headers.containsKey(sampleName))
			return headers.get(sampleName);
		return null;
	}
	
	public void headersConfig(List<Path> paths,String outputDir,Configuration conf){
		getHeaders(paths);
		writeHeaders(outputDir,conf);
	}

	public void getHeaders(List<Path> paths) {
		for (Path p : paths) {
			VCFHdfsLoader loader = null;
			try {
				loader = new VCFHdfsLoader(p.toString());
			} catch (IllegalArgumentException | IOException e) {
				e.printStackTrace();
			}
			if (loader.getHeader().getSampleNamesInOrder().size() == 0)
				throw new RuntimeException("VCF header contains no samples!");
			String name = loader.getHeader().getSampleNamesInOrder().get(0);

			if (headers.containsKey(name))
				throw new RuntimeException("more than one VCF header contains same sample name!");
			headers.put(name, loader.getHeader());
			loader.close();
		}
	}

	public void writeHeaders(String outputDir,Configuration conf) {
		conf.set(HEADER_DEFAULT_PATH, outputDir);
		if(!outputDir.endsWith("/"))
			outputDir = outputDir+"/";
		for (String name : headers.keySet()) {
			VCFHdfsWriter vcfHdfsWriter = null;
			try {
				vcfHdfsWriter = new VCFHdfsWriter(outputDir+name, false, false, conf);
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
	        vcfHdfsWriter.writeHeader(headers.get(name));
	        vcfHdfsWriter.close();
		}
	}
	
	public VCFHeader readHeader(Path path,Configuration conf) throws IOException, ClassNotFoundException{
		SeekableStream in = WrapSeekable.openPath(path.getFileSystem(conf), path);
		VCFHeader header = VCFHeaderReader.readHeaderFrom(in);
		in.close();
		
		return header;
	}
	
	public void readHeaders(String outputDir,Configuration conf){
		Path path = new Path(outputDir);
		
		FileSystem fs = null;
		try {
			fs = path.getFileSystem(conf);
		
			if(fs.isFile(path)){
				headers.put(path.getName(), readHeader(path,conf));
			} else{
				FileStatus stats[] = fs.listStatus(path);
				
				for (FileStatus file : stats) {
					Path filePath = file.getPath();
					if(!fs.isFile(filePath))
						throw new RuntimeException("cann't not support sub dir");
					headers.put(filePath.getName(), readHeader(filePath,conf));
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e.toString());
		}
	}
	
	public void readHeaders(Configuration conf){
		String outputDir = conf.get(HEADER_DEFAULT_PATH);
		readHeaders(outputDir,conf);
	}
	
	public Set<VCFHeader> getHeaders(){
		Set<VCFHeader> headerSet = new HashSet<VCFHeader>();
		for(VCFHeader header : headers.values()){
			headerSet.add(header);
		}
		return headerSet;
	}
	
	public void clear(){
		headers.clear();
	}
	
	public Set<String> keySet(){
		return this.headers.keySet();
	}
}
