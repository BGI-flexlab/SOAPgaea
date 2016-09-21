package org.bgi.flexlab.gaea.data.structure.pileup.filter;

import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;

public class PileupElementBaseAndMappingQualityFilter implements
		PileupElementFilter {
	private int minBaseQuality;
	private int minMappingQuality;

	public PileupElementBaseAndMappingQualityFilter() {
		this.minBaseQuality = 17;
		this.minMappingQuality = 0;
	}

	public PileupElementBaseAndMappingQualityFilter(int BQ, int MQ) {
		this.minBaseQuality = BQ;
		this.minMappingQuality = MQ;
	}

	@Override
	public boolean allow(PileupElement pileupElement) {
		if (pileupElement.getMappingQuality() >= this.minMappingQuality
				&& pileupElement.getQuality() >= this.minBaseQuality)
			return true;
		return false;
	}

}
