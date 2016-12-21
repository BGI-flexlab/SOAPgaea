package org.bgi.flexlab.gaea.data.structure.pileup;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bgi.flexlab.gaea.data.exception.UserException;

abstract class PileupElementTracker<PE extends PileupElement> implements
		Iterable<PE> {
	public abstract int size();

	public abstract Iterator<PE> iterator();
}

class UnifiedPileupElementTracker<PE extends PileupElement> extends
		PileupElementTracker<PE> {
	private final List<PE> piltupList;

	public UnifiedPileupElementTracker() {
		piltupList = new LinkedList<PE>();
	}

	public UnifiedPileupElementTracker(List<PE> pileup) {
		this.piltupList = pileup;
	}

	public void add(PE element) {
		piltupList.add(element);
	}

	public PE get(int index) {
		return piltupList.get(index);
	}

	public int size() {
		return piltupList.size();
	}

	public Iterator<PE> iterator() {
		return piltupList.iterator();
	}
}

class SamplePileupElementTracker<PE extends PileupElement> extends
		PileupElementTracker<PE> {
	private final Map<String, PileupElementTracker<PE>> pileupMap;
	private int size = 0;

	public SamplePileupElementTracker() {
		pileupMap = new HashMap<String, PileupElementTracker<PE>>();
	}

	public Collection<String> getSamples() {
		return pileupMap.keySet();
	}

	public PileupElementTracker<PE> getElements(final String sample) {
		return pileupMap.get(sample);
	}

	public PileupElementTracker<PE> getElements(
			final Collection<String> selectSampleNames) {
		SamplePileupElementTracker<PE> result = new SamplePileupElementTracker<PE>();
		for (final String sample : selectSampleNames) {
			result.addElements(sample, pileupMap.get(sample));
		}
		return result;
	}

	public void addElements(final String sample,
			PileupElementTracker<PE> elements) {
		if (pileupMap.containsKey(sample))
			throw new UserException.PileupException("add elements for sample "
					+ sample + " is duplication?");
		pileupMap.put(sample, elements);
		size += elements.size();
	}

	public Iterator<PE> iterator() {
		return new SamplesPileupIterator<PE>(this);
	}

	public int sampleSize() {
		return pileupMap.keySet().size();
	}

	public int size() {
		return size;
	}
}
