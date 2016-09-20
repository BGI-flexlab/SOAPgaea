package org.bgi.flexlab.gaea.data.structure.pileup.filter;

import java.util.HashSet;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;

public class PileupElementReadGroupsFilter implements PileupElementFilter {
	private HashSet<String> readGroupIDs = null;

	public PileupElementReadGroupsFilter(String readGroupId) {
		if (readGroupId != null && readGroupId != "") {
			readGroupIDs = new HashSet<String>();
			readGroupIDs.add(readGroupId);
		}
	}

	public PileupElementReadGroupsFilter(HashSet<String> readGroupIds) {
		this.readGroupIDs = readGroupIds;
	}

	@Override
	public boolean allow(PileupElement pileupElement) {
		GaeaSamRecord read = pileupElement.getRead();
		if (readGroupIDs != null) {
			if (read.getReadGroup() != null
					&& readGroupIDs.contains(read.getReadGroup()
							.getReadGroupId()))
				return true;
		} else {
			if (read.getReadGroup() == null
					|| read.getReadGroup().getReadGroupId() == null)
				return true;
		}
		return false;
	}

}
