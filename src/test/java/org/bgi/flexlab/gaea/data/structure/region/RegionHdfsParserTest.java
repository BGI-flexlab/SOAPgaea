package org.bgi.flexlab.gaea.data.structure.region;

import java.io.IOException;

import org.bgi.flexlab.gaea.data.mapreduce.input.bed.RegionHdfsParser;
import org.junit.Test;

import junit.framework.Assert;

public class RegionHdfsParserTest {
	@Test
	public void parseBedFileFromHDFSTest() throws IOException {
		RegionHdfsParser rg = new RegionHdfsParser();
		rg.parseBedFileFromHDFS("MedExome_hg19_UCSCBrowser.bed", true);
		Assert.assertEquals(112214187, rg.getRegionSize());
		Assert.assertEquals(257268185, rg.getRegionFlankSize());
		Assert.assertTrue(rg.isPositionInRegion("chr9", 139350039));
		Assert.assertTrue(rg.isPositionInFlank("chr1", 70118));
		Assert.assertTrue(rg.isReadInRegion("chr1", 10041045, 10041147));
		Assert.assertEquals("chr1", 10163009, rg.getChrSize("chr1"));
		Assert.assertEquals("chrY", 304415, rg.getChrSize("chrY"));
	}

}
