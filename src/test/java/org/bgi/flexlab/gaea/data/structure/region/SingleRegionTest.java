package org.bgi.flexlab.gaea.data.structure.region;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;

public class SingleRegionTest {

	@Test
	public void test() throws IOException {
		SingleRegion sg = new SingleRegion();
		String file = "MedExome_hg19_UCSCBrowser.bed";
		sg.parseRegionsFileFromHDFS(file, true, 200);
		Assert.assertEquals("regions size", 418319, sg.getRegions().size());
		Assert.assertEquals("chrNameInterval size " ,24, sg.chrNameInterval.keySet().size());
		Assert.assertEquals("region 1", "chr1,69490,70224", sg.getRegion(3).getChrName()
				+ "," + sg.getRegion(3).getStart() + "," +
				sg.getRegion(3).getEnd());
	}

}
