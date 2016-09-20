package org.bgi.flexlab.gaea.data.structure.pileup.filter;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;

public class PileupElementLaneFilter implements PileupElementFilter {
	private String laneID;

	public PileupElementLaneFilter() {
		laneID = null;
	}

	public PileupElementLaneFilter(String laneId) {
		this.laneID = laneId;
	}

	@Override
	public boolean allow(PileupElement pileupElement) {
		GaeaSamRecord read = pileupElement.getRead();
		if (laneID != null) {
			if (read.getReadGroup() != null
					&& (read.getReadGroup().getReadGroupId().startsWith(laneID
							+ "."))
					|| (read.getReadGroup().getReadGroupId().equals(laneID)))
				return true;
		} else {
			if (read.getReadGroup() == null
					|| read.getReadGroup().getReadGroupId() == null)
				return true;
		}
		return false;
	}

}
