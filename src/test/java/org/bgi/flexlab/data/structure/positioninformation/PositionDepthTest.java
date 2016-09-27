package org.bgi.flexlab.data.structure.positioninformation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.data.structure.reads.BamQCReadInformation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.junit.Test;

import junit.framework.Assert;

public class PositionDepthTest {

	@Test
	public void test() throws IOException {
		BamQCPositionDepth depth = new BamQCPositionDepth(1000000, true, true, 8);
		GenomeShare genome = new GenomeShare();
		genome.loadChromosomeList("file:///ifs4/ISDC_BD/GaeaProject/reference/hg19/GaeaIndex/ref_bn.list");
		ChromosomeInformationShare chrInfo = genome.getChromosomeInfo("chr1");
		BufferedReader reader = new BufferedReader(new FileReader(new File("/ifs4/ISDC_BD/huweipeng/data/testSam/bamQC_tempfile2")));
		BamQCReadInformation readInfo = new BamQCReadInformation();
		String line = null;
		while((line = reader.readLine()) != null) {
			if(!readInfo.parseBAMQC(line))
				continue;
			depth.add(new CompoundInformation<BamQCReadInformation>(0, 1000000, readInfo, chrInfo));
		}
		reader.close();
		Assert.assertEquals("Position 501232 depth:", 9, depth.getPosDepth(501232));
		Assert.assertEquals("Rmdup Position 501232 depth:", 9, depth.getRMDupPosDeepth(501232));
		Assert.assertEquals("Gender Position 501232 depth:", 5, depth.getGenderUesdDepth(501232));
		Assert.assertEquals("Rmdup Position 501232 depth:", 95, depth.getRMDupPosDeepth(50123));
		Assert.assertEquals("Lane depth:", 17, depth.getLaneDepth(3)[0]);
		Assert.assertEquals("Lane depth:", 13, depth.getLaneDepth(3)[1]);
		Assert.assertFalse("Pos 12 has indel reads:", depth.hasIndelReads(12));
		Assert.assertTrue("Pos 12 has indel reads:", depth.hasIndelReads(34));
		Assert.assertTrue("Pos 33 has mismatch reads:", depth.hasMismatchReads(33));
		Assert.assertTrue("Pos 12345 has mismatch reads:", depth.hasMismatchReads(12345));
		Assert.assertTrue("Pos 31678 is deletion base with no conversion:", depth.isDeletionBaseWithNoConver(31678));
		Assert.assertTrue("Pos 40550 is deletion base with no conversion:", depth.isDeletionBaseWithNoConver(40550));
		Assert.assertFalse("Pos 43897 is deletion base with no conversion:", depth.isDeletionBaseWithNoConver(43897));
	}

}
