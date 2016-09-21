package org.bgi.flexlab.gaea.data.structure.pileup.filter;

import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;

/**
 * A filtering interface for pileup elements.
 */
public interface PileupElementFilter {
	public boolean allow(final PileupElement pileupElement);
}
