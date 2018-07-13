package org.bgi.flexlab.gaea.tools.jointcalling.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import htsjdk.variant.vcf.VCFHeaderLine;
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
	
	class VCFHeaderWithIndex{
		public int index;
		public VCFHeader header;
		
		public VCFHeaderWithIndex(VCFHeader header,int index){
			this.header = header;
			this.index = index;
		}
	}

	private static final long serialVersionUID = -3352974548797591555L;

	private HashMap<String, VCFHeaderWithIndex> headers = new HashMap<String, VCFHeaderWithIndex>();
	
	private String HEADER_DEFAULT_PATH = "vcfHeaders";
	
	private int currentIndex = 0;

	public MultipleVCFHeaderForJointCalling() {
	}

	public VCFHeader getVCFHeader(String sampleName) {
		if (headers.containsKey(sampleName))
			return headers.get(sampleName).header;
		return null;
	}
	
	public void headersConfig(List<Path> paths,String outputDir,Configuration conf){
		getHeaders(paths);
		writeHeaders(outputDir,conf);
	}

	public void headersConfig(VCFHeader header,String outputDir,Configuration conf){
		getHeaders(header);
		writeHeaders(outputDir,conf);
	}

	public void headersConfig(Path headerPath,String outputDir,Configuration conf){
		getHeaders(headerPath, conf);
		writeHeaders(outputDir,conf);
	}

	public void getHeaders(List<Path> paths) {
		currentIndex = 0;
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
			
			VCFHeaderWithIndex headerWithIndex = new VCFHeaderWithIndex(loader.getHeader(),currentIndex);
			currentIndex++;
			headers.put(name, headerWithIndex);
			loader.close();
		}
	}

	public void getHeaders(VCFHeader header) {
		currentIndex = 0;
		Set<VCFHeaderLine> vcfHeaderLines = header.getMetaDataInInputOrder();
		for(String sample: header.getSampleNamesInOrder()){
			Set<String> sampleSet = new TreeSet<>();
			sampleSet.add(sample);
			VCFHeader vcfHeader = new VCFHeader(vcfHeaderLines, sampleSet);
			VCFHeaderWithIndex headerWithIndex = new VCFHeaderWithIndex(vcfHeader,currentIndex);
			currentIndex++;
			headers.put(sample, headerWithIndex);
		}
	}

	public void getHeaders(Path headerPath, Configuration conf) {
		VCFHeader header = null;
		try {
			header = readHeader(headerPath, conf);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		currentIndex = 0;
		Set<VCFHeaderLine> vcfHeaderLines = header.getMetaDataInInputOrder();
		for(String sample: header.getSampleNamesInOrder()){
			Set<String> sampleSet = new TreeSet<>();
			sampleSet.add(sample);
			VCFHeader vcfHeader = new VCFHeader(vcfHeaderLines, sampleSet);
			VCFHeaderWithIndex headerWithIndex = new VCFHeaderWithIndex(vcfHeader,currentIndex);
			currentIndex++;
			headers.put(sample, headerWithIndex);
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
	        vcfHdfsWriter.writeHeader(headers.get(name).header);
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
		
		currentIndex = 0;
		FileSystem fs = null;
		try {
			fs = path.getFileSystem(conf);
		
			if(fs.isFile(path)){
				VCFHeaderWithIndex headerWithIndex = new VCFHeaderWithIndex(readHeader(path,conf),0);
				headers.put(path.getName(), headerWithIndex);
			} else{
				FileStatus stats[] = fs.listStatus(path);
				
				for (FileStatus file : stats) {
					Path filePath = file.getPath();
					if(!fs.isFile(filePath))
						throw new RuntimeException("cann't not support sub dir");
					VCFHeaderWithIndex headerWithIndex = new VCFHeaderWithIndex(readHeader(filePath,conf),currentIndex);
					headers.put(filePath.getName(), headerWithIndex);
					currentIndex++;
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
		for(VCFHeaderWithIndex headerWithIndex : headers.values()){
			headerSet.add(headerWithIndex.header);
		}
		return headerSet;
	}
	
	public String[] getSamplesAsInputOrder(){
		String[] samples = new String[headers.size()];
		
		for(VCFHeaderWithIndex headerWithIndex : headers.values()){
			samples[headerWithIndex.index] = headerWithIndex.header.getSampleNamesInOrder().get(0);
		}
		
		return samples;
	}
	
	public void clear(){
		headers.clear();
	}
	
	public Set<String> keySet(){
		return this.headers.keySet();
	}
}
