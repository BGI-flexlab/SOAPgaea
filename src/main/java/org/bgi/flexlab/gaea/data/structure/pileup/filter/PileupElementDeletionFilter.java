package org.bgi.flexlab.gaea.data.structure.pileup.filter;

import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;

public class PileupElementDeletionFilter implements PileupElementFilter{

	@Override
	public boolean allow(PileupElement pileupElement) {
		if(pileupElement.isDeletion())
			return false;
		return true;
	}

}
