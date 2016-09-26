package org.bgi.flexlab.gaea.data.structure.region;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;

public class RegionLocalParserTest {

	@Test
	public void parseBedFileTest() throws IOException {
		RegionLocalParser rg = new RegionLocalParser();
		rg.parseBedFile("F:\\BGIBigData\\TestData\\Bed\\MedExome_hg19_UCSCBrowser.bed", true);
		Assert.assertEquals(112214187, rg.getRegionSize());
		Assert.assertEquals(257268185, rg.getRegionFlankSize());
		Assert.assertTrue(rg.isPositionInRegion("chr9", 139350039));
		Assert.assertTrue(rg.isPositionInFlank("chr1", 70118));
		Assert.assertTrue(rg.isReadInRegion("chr1", 10041045, 10041147));
	}
	

}
