package org.bgi.flexlab.gaea.data.structure.pileup;

import htsjdk.samtools.util.PeekableIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MultipleSamplesPileupElementIterator<PE extends PileupElement>
		implements Iterator<PE> {
	private final PriorityQueue<PeekableIterator<PE>> sampleIterators;

	public MultipleSamplesPileupElementIterator(
			SamplePileupElementTracker<PE> tracker) {
		sampleIterators = new PriorityQueue<PeekableIterator<PE>>(Math.max(1,
				tracker.sampleSize()), new PileupElementIteratorComparator());
		for (final String sample : tracker.getSamples()) {
			PileupElementTracker<PE> trackerPerSample = tracker
					.getElements(sample);
			if (trackerPerSample.size() != 0)
				sampleIterators.add(new PeekableIterator<PE>(trackerPerSample
						.iterator()));
		}
	}

	public boolean hasNext() {
		return !sampleIterators.isEmpty();
	}

	public PE next() {
		PeekableIterator<PE> currentIterator = sampleIterators.peek();
		PE current = currentIterator.next();
		if (!currentIterator.hasNext())
			sampleIterators.remove();
		return current;
	}

	public void remove() {
		throw new UnsupportedOperationException(
				"Cannot remove from a multiple sample iterator.");
	}

	/**
	 * Compares two peekable iterators consisting of pileup elements.
	 */
	private class PileupElementIteratorComparator implements
			Comparator<PeekableIterator<PE>> {
		public int compare(PeekableIterator<PE> lhs, PeekableIterator<PE> rhs) {
			return rhs.peek().getOffset() - lhs.peek().getOffset();
		}
	}
}
