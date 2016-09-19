package org.bgi.flexlab.gaea.tools.vcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.format.vcf.header.GaeaSingleVCFHeader;
import org.junit.Test;
import junit.framework.Assert;

public class VCFSortTest {

	@Test
	public void test() {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");
		Path input = new Path("file:///ifs4/ISDC_BD/huweipeng/data/testVCF/");
		GaeaSingleVCFHeader header = VCFSort.getVcfHeader(input, conf);
		for(String s : header.getSampleNames()) {
			System.out.println(s);
		}
		Assert.assertEquals(2, header.getSampleNames().size());
	}

}


