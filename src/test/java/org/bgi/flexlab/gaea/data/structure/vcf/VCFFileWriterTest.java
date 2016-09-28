package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.VCFHdfsWriter;
import org.bgi.flexlab.gaea.data.structure.vcf.GaeaVCFCodec;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

import org.junit.Test;

import junit.framework.Assert;

public class VCFFileWriterTest {
	GaeaVCFCodec codec;
//
//	@Test
//	public void writeHeaderToLocalTest() throws IOException {
//		String filePath = "F:\\BGIBigData\\TestData\\VCF\\localHeader";
//		VCFLocalWriter writer = new VCFLocalWriter(filePath, false, false);
//		Assert.assertEquals(29, writeHeader(filePath, writer));
//	}

	@Test
	public void writeHeaderToHDFSTest() throws IOException {
		String filePath = "HDFSHeader";
		VCFHdfsWriter writer = new VCFHdfsWriter(filePath, false, false, new Configuration(false));
		Assert.assertEquals(29, writeHeader(filePath, writer));
	}
	
//	@Test
//	public void addToLocalTest() throws IOException {
//		String filePath = "F:\\BGIBigData\\TestData\\VCF\\localDNA1425995.vcf";
//		VCFLocalWriter writer = new VCFLocalWriter(filePath, false, false);
//		int[] result = add(filePath, writer);
//		Assert.assertEquals(result[0], result[1]);
//
//	}
	
	@Test
	public void addToHDFSTest() throws IOException {
		String filePath = "HDFSDNA1425995.vcf";
		VCFHdfsWriter writer = new VCFHdfsWriter(filePath, false, false, new Configuration(false));
		int[] result = add(filePath, writer);
		Assert.assertEquals(result[0], result[1]);
	}
	
	private VCFHeader getHeader() throws IOException{
		VCFHeader header;
		String input = "/ifs4/ISDC_BD/huweipeng/data/testVCF/DNA1425995.vcf";
		BufferedReader reader=null;
		codec = new GaeaVCFCodec();
		ArrayList<String> headerLine=new ArrayList<String>();
    	File file=new File(input);
		reader=new BufferedReader(new FileReader(file));
		String tempString=null;
		
		//read header
		while((tempString=reader.readLine())!=null) {
			if(tempString.startsWith("#"))
			{
				headerLine.add(tempString.trim());
			}else
				break;
		}
		header=(VCFHeader)codec.readHeader(headerLine);
		return header;
	}
	
	private int writeHeader(String path, VCFFileWriter writer) throws IOException{
		String filePath = path;
		writer.writeHeader(getHeader());
		writer.close();
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		int lines = 0;
		lines = getHeaderLines(reader);
		reader.close();
		return lines;
	}
	
	private int getHeaderLines(BufferedReader reader) throws IOException{
		int lines = 0;
		while((reader.readLine())!=null){
			lines += 1;
		}
		return lines;
	}
	
	private int[] add(String path, VCFFileWriter writer) throws IOException{
		String filePath = path;
		int[] result = new int[2];
		writer.writeHeader(getHeader());
		BufferedReader reader = new BufferedReader(new FileReader(
				new File( "/ifs4/ISDC_BD/huweipeng/data/testVCF/DNA1425995.vcf")));
		String line = null;
		int count1 = 0;
		while((line = reader.readLine()) != null) {
			count1 += 1;
			VariantContext vc = codec.decode(line);
			if(vc == null) {
				continue;
			} else {
				writer.add(vc);
			}
		}
		result[0] = count1;
		reader.close();
		writer.close();
		BufferedReader reader2 = new BufferedReader(new FileReader(new File(filePath)));
		int count2 = 0;
		while((reader2.readLine())!=null){
			count2 += 1;
		}
		result[1] = count2;
		reader2.close();
		return result;
	}
}
