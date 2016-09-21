package org.bgi.flexlab.gaea.data.structure.header;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

public class GaeaSingleVCFHeader extends GaeaVCFHeader implements Serializable{
	/**
	 * serial id
	 */
	private static final long serialVersionUID = -5010893132552487497L;
	
	/**
	 * sample names
	 */
	private List<String> sampleNames = new ArrayList<String>();
	
	/**
	 * basic header string without sample names 
	 */
	private String headerInfo;
	
	/**
	 * parse header from given VCF files
	 * @param inVcf
	 * @param output
	 * @param conf
	 * @throws IOException
	 */
	public void parseHeader(Path vcf, String output, Configuration conf) throws IOException {
		readSingleHeader(vcf, conf);
		
		if(output != null) {//multi-sample do not write to HDFS, top class will handle this.
			writeHeaderToHDFS(output);
		}
	}
	
	public void readSingleHeader(Path vcfPath, Configuration conf) throws IOException {
		FileSystem fs = vcfPath.getFileSystem(conf);
		if(!fs.exists(vcfPath)) {
			throw new RuntimeException(vcfPath.toString() + " don't exist.");
		}
		if(!fs.isFile(vcfPath)) {
			throw new RuntimeException(vcfPath.toString() + " is not a file. GaeaSingleVcfHeader parser only support one vcf file.");
		}
		
		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
	    CompressionCodec codec = compressionCodecs.getCodec(vcfPath);
		FSDataInputStream in = fs.open(vcfPath);
		LineReader reader;
		if(codec != null){
			reader = new LineReader(codec.createInputStream(in), conf);
		} else {
			reader = new LineReader(in, conf);
		}
		
		Text textLine = new Text();
		StringBuilder header = new StringBuilder();
		while(reader.readLine(textLine) >= 0) {
			String line = textLine.toString();
			if(line.startsWith("##")) {
				collectMetaInfo(header, line);
			} else if(line.startsWith("#")) {
					collectHeader(header, line);
					headerInfo = header.toString();
					collectSamples(line);
			} else {
				break;
			}
		}
		reader.close();
	}
	
	private void collectMetaInfo(StringBuilder header, String line){
		header.append(line.trim());
		header.append("\n");
	}
	
	private void collectHeader(StringBuilder header, String line){
		String[] tags = line.trim().split("\t");
		header.append(tags[0]);
		for(int i = 1; i < 9; i++) {
			header.append("\t");
			header.append(tags[i]);
		}
	}
	
	private void collectSamples(String line){
		String[] lineSplits = line.split("\t");
		for(int i = 9; i < lineSplits.length; i++) {
			sampleNames.add(lineSplits[i]);
		}
	}
	
	public String getHeaderInfoWithSample(List<String> samples) {
		if(samples == null) {
			samples = this.sampleNames;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(headerInfo);
		for(String sample : samples) {
			sb.append("\t");
			sb.append(sample);
		}
		sb.append("\n");
		return sb.toString();
	}
	
	public String[] getHeaderInfoStringLines(List<String> samples) {
		return getHeaderInfoWithSample(samples).split("\n");
	}
	
	public String getSampleNames(int index) {
		return sampleNames.get(index);
	} 
	
	@Override 
	public void copy(GaeaVCFHeader header) {
		GaeaSingleVCFHeader singleHeader = (GaeaSingleVCFHeader) header;
		sampleNames = singleHeader.getSampleNames();
		headerInfo = singleHeader.getHeaderInfo();
	}
	
	@Override
	public GaeaVCFHeader initializeHeader(){
		return new GaeaSingleVCFHeader();
	}
	
	public List<String> getSampleNames() {
		return sampleNames;
	}
	
	public String getHeaderInfo() {
		return headerInfo;
	}

}
