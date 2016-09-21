package org.bgi.flexlab.gaea.structure.header;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.structure.header.GaeaSingleVCFHeader;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class GaeaSingleVCFHeaderTest {
	GaeaSingleVCFHeader gmv;
	String output;
	Configuration conf;
	Path inputPath;
	@Before
	public void setup(){
		gmv = new GaeaSingleVCFHeader();
		output = "testOutput2";
		conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");
		inputPath = new Path("file:///ifs4/ISDC_BD/huweipeng/data/testVCF/DNA1425995.vcf");
	}

	@Test
	public void loadVcfHeader() throws IOException{
		gmv.parseHeader(inputPath, output, conf);
		gmv.loadVcfHeader(output);
		for(String file:gmv.getSampleNames()){
			System.out.println(file);
		}
		Assert.assertEquals(gmv.getSampleNames().size(), 1);
	}
	
	@Test
	public void readSingleHeader() throws IOException{
		gmv.readSingleHeader(inputPath, conf);
		String[] headerLines = gmv.getHeaderInfo().split("\n");
		for(String field : headerLines[headerLines.length - 1].split("\t")){
			System.out.println(field);
		}
		Assert.assertEquals(9, headerLines[headerLines.length - 1].split("\t").length);
		Assert.assertEquals(1, gmv.getSampleNames().size());
	}
}
