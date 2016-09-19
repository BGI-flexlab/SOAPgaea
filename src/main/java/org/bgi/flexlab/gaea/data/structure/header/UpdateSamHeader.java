package org.bgi.flexlab.gaea.data.structure.header;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;

import java.util.ArrayList;
import java.util.List;

public class UpdateSamHeader {
	public static SAMFileHeader createHeaderFromSampleName(
			SAMFileHeader header, String sampleName) {
		SAMFileHeader newHeader = header.clone();
		List<SAMReadGroupRecord> groups = header.getReadGroups();
		List<SAMReadGroupRecord> rgroups = new ArrayList<SAMReadGroupRecord>(
				groups.size());

		for (SAMReadGroupRecord g : groups) {
			if (g.getSample().equals(sampleName))
				rgroups.add(g);
		}
		newHeader.setReadGroups(rgroups);
		return newHeader;
	}
}
