package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.data.structure.reads.ReadInformationForBamQC;
import org.bgi.flexlab.gaea.data.structure.reads.ReadInformationForBamToDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.junit.Test;

import junit.framework.Assert;

public class PositionDepthTest {

	
	public void PostionDepthForBamQCTest() throws IOException {
		PositionDepth depth = new PositionDepth(1000000, true, 8);
		GenomeShare genome = new GenomeShare();
		genome.loadChromosomeList("file:///ifs4/ISDC_BD/GaeaProject/reference/hg19/GaeaIndex/ref_bn.list");
		ChromosomeInformationShare chrInfo = genome.getChromosomeInfo("chr1");
		BufferedReader reader = new BufferedReader(new FileReader(new File("/ifs4/ISDC_BD/huweipeng/data/testSam/bamQC_tempfile2")));
		ReadInformationForBamQC readInfo = new ReadInformationForBamQC();
		String line = null;
		while((line = reader.readLine()) != null) {
			if(!readInfo.parseBAMQC(line))
				continue;
			depth.add(new CompoundInformation<ReadInformationForBamQC>(0, 1000000, readInfo, chrInfo));
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
	
	@Test
	public void PostionDepthForBam2DepthTest() throws IOException {
		PositionDepth depth = new PositionDepth(1);
		BufferedReader reader = new BufferedReader(new FileReader(new File("/ifs4/ISDC_BD/huweipeng/data/testSam/Bam2Depth_tempfile")));
		ReadInformationForBamToDepth readInfo = new ReadInformationForBamToDepth();
		String line = null;
		while((line = reader.readLine()) != null) {
			if(!readInfo.parseBam2Depth(line))
				continue;
			depth.add(0, 0, readInfo);
		}
		reader.close();
		
		Assert.assertEquals("Position 86 depth:", 7, depth.getPosDepthSamtools(0, 86));
		Assert.assertEquals("Position 90 depth:", 7, depth.getPosDepthSamtools(0, 90));
		Assert.assertEquals("Position 91 depth:", 9, depth.getPosDepthSamtools(0, 91));
		Assert.assertEquals("Position 96 depth:", 9, depth.getPosDepthSamtools(0, 96));
		Assert.assertEquals("Position 4984 depth:", 41, depth.getPosDepthSamtools(0, 4984));
		Assert.assertEquals("Position 4985 depth:", 42, depth.getPosDepthSamtools(0, 4985));

	}

}
