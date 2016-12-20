package org.bgi.flexlab.gaea.data.structure.pileup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.pileup.filter.PileupElementFilter;
import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.FragmentCollection;
import org.bgi.flexlab.gaea.util.FragmentUtils;

public abstract class AbstractPileup<AP extends AbstractPileup<AP, PE>, PE extends PileupElement>
		implements Pileup {
	protected final GenomeLocation location;
	protected final PileupElementTracker<PE> pileupElementTracker;

	protected int size = 0; // cached value of the size of the pileup
	protected int abstractSize = -1; // cached value of the abstract size of the
										// pileup
	protected int nDeletions = 0; // cached value of the number of deletions
	protected int nMQ0Reads = 0; // cached value of the number of MQ0 reads

	/**
	 * Create a new version of a read backed pileup at loc, using the reads and
	 * their corresponding offsets.
	 */
	public AbstractPileup(GenomeLocation location, List<GaeaSamRecord> reads,
			List<Integer> offsets) {
		this.location = location;
		this.pileupElementTracker = readsOffsets2Pileup(reads, offsets);
	}

	/**
	 * Create a new version of a read backed pileup at loc without any aligned
	 * reads
	 */
	public AbstractPileup(GenomeLocation location) {
		this(location, new UnifiedPileupElementTracker<PE>());
	}

	/**
	 * Create a new version of a read backed pileup at loc, using the reads and
	 * their corresponding offsets. This lower level constructure assumes pileup
	 * is well-formed and merely keeps a pointer to pileup. Don't go changing
	 * the data in pileup.
	 */
	public AbstractPileup(GenomeLocation location, List<PE> pileup) {
		if (location == null)
			throw new UserException.PileupException(
					"Illegal null genome location in pileup");
		if (pileup == null)
			throw new UserException.PileupException(
					"Illegal null pileup in abstract pileup");

		this.location = location;
		this.pileupElementTracker = new UnifiedPileupElementTracker<PE>(pileup);
		calculateCachedData();
	}

	/**
	 * Optimization of above constructor where all of the cached data is
	 * provided
	 */
	public AbstractPileup(GenomeLocation location, List<PE> pileup, int size,
			int nDeletions, int nMQ0Reads) {
		if (location == null)
			throw new UserException.PileupException(
					"Illegal null genome location in UnifiedReadBackedPileup");
		if (pileup == null)
			throw new UserException.PileupException(
					"Illegal null pileup in UnifiedReadBackedPileup");

		this.location = location;
		this.pileupElementTracker = new UnifiedPileupElementTracker<PE>(pileup);
		this.size = size;
		this.nDeletions = nDeletions;
		this.nMQ0Reads = nMQ0Reads;
	}

	protected AbstractPileup(GenomeLocation location,
			PileupElementTracker<PE> tracker) {
		this.location = location;
		this.pileupElementTracker = tracker;
		calculateCachedData();
	}

	protected AbstractPileup(GenomeLocation location,
			Map<String, ? extends AbstractPileup<AP, PE>> pileupsBySample) {
		this.location = location;
		SamplePileupElementTracker<PE> tracker = new SamplePileupElementTracker<PE>();
		for (Map.Entry<String, ? extends AbstractPileup<AP, PE>> pileupEntry : pileupsBySample
				.entrySet()) {
			tracker.addElements(pileupEntry.getKey(),
					pileupEntry.getValue().pileupElementTracker);
			addPileupToCumulativeStats(pileupEntry.getValue());
		}
		this.pileupElementTracker = tracker;
	}

	public AbstractPileup(GenomeLocation location, List<GaeaSamRecord> reads,
			int offset) {
		this.location = location;
		this.pileupElementTracker = readsOffsets2Pileup(reads, offset);
	}

	/**
	 * Calculate cached sizes, nDeletion, and base counts for the pileup. This
	 * calculation is done upfront, so you pay the cost at the start, but it's
	 * more efficient to do this rather than pay the cost of calling sizes,
	 * nDeletion, etc. over and over potentially.
	 */
	protected void calculateCachedData() {
		size = 0;
		nDeletions = 0;
		nMQ0Reads = 0;

		for (PileupElement p : pileupElementTracker) {
			size++;
			if (p.isDeletion()) {
				nDeletions++;
			}
			if (p.getRead().getMappingQuality() == 0) {
				nMQ0Reads++;
			}
		}
	}

	protected void calculateAbstractSize() {
		abstractSize = 0;
		for (PileupElement p : pileupElementTracker) {
			abstractSize += p.getRepresentativeCount();
		}
	}

	protected void addPileupToCumulativeStats(AbstractPileup<AP, PE> pileup) {
		size += pileup.getNumberOfElements();
		abstractSize = pileup.depthOfCoverage()
				+ (abstractSize == -1 ? 0 : abstractSize);
		nDeletions += pileup.getNumberOfDeletions();
		nMQ0Reads += pileup.getNumberOfMappingQualityZeroReads();
	}

	/**
	 * Helper routine for converting reads and offset lists to a PileupElement
	 * list.
	 */
	private PileupElementTracker<PE> readsOffsets2Pileup(
			List<GaeaSamRecord> reads, List<Integer> offsets) {
		if (reads == null)
			throw new UserException.PileupException(
					"Illegal null read list in UnifiedReadBackedPileup");
		if (offsets == null)
			throw new UserException.PileupException(
					"Illegal null offsets list in UnifiedReadBackedPileup");
		if (reads.size() != offsets.size())
			throw new UserException(
					"Reads and offset lists have different sizes!");

		UnifiedPileupElementTracker<PE> pileup = new UnifiedPileupElementTracker<PE>();
		for (int i = 0; i < reads.size(); i++) {
			GaeaSamRecord read = reads.get(i);
			int offset = offsets.get(i);
			pileup.add(createNewPileupElement(read, offset, false, false,
					false, false, false, false));
		}

		return pileup;
	}

	/**
	 * Helper routine for converting reads and a single offset to a
	 * PileupElement list.
	 */
	private PileupElementTracker<PE> readsOffsets2Pileup(
			List<GaeaSamRecord> reads, int offset) {
		if (reads == null)
			throw new UserException.PileupException(
					"Illegal null read list in UnifiedReadBackedPileup");
		if (offset < 0)
			throw new UserException.PileupException(
					"Illegal offset < 0 UnifiedReadBackedPileup");

		UnifiedPileupElementTracker<PE> pileup = new UnifiedPileupElementTracker<PE>();
		for (GaeaSamRecord read : reads) {
			pileup.add(createNewPileupElement(read, offset, false, false,
					false, false, false, false)); 
		}

		return pileup;
	}

	protected abstract AbstractPileup<AP, PE> createNewPileup(
			GenomeLocation location, PileupElementTracker<PE> pileupElementTracker);

	protected abstract PE createNewPileupElement(final GaeaSamRecord read,
			final int offset, final boolean isDeletion,
			final boolean isBeforeDeletion, final boolean isAfterDeletion,
			final boolean isBeforeInsertion, final boolean isAfterInsertion,
			final boolean isNextToSoftClip);

	protected abstract PE createNewPileupElement(final GaeaSamRecord read,
			final int offset, final boolean isDeletion,
			final boolean isBeforeDeletion, final boolean isAfterDeletion,
			final boolean isBeforeInsertion, final boolean isAfterInsertion,
			final boolean isNextToSoftClip, final String nextEventBases,
			final int nextEventLength);

	/**
	 * Returns a new ReadBackedPileup where only one read from an overlapping
	 * read pair is retained. If the two reads in question disagree to their
	 * basecall, neither read is retained. If they agree on the base, the read
	 * with the higher quality observation is retained
	 */
	@SuppressWarnings("unchecked")
	@Override
	public AP getOverlappingFragmentFilteredPileup() {
		if (pileupElementTracker instanceof SamplePileupElementTracker) {
			SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
			SamplePileupElementTracker<PE> filteredTracker = new SamplePileupElementTracker<PE>();

			for (final String sample : tracker.getSamples()) {
				PileupElementTracker<PE> perSampleElements = tracker
						.getElements(sample);
				AbstractPileup<AP, PE> pileup = createNewPileup(
						location, perSampleElements)
						.getOverlappingFragmentFilteredPileup();
				filteredTracker
						.addElements(sample, pileup.pileupElementTracker);
			}
			return (AP) createNewPileup(location, filteredTracker);
		} else {
			Map<String, PE> filteredPileup = new HashMap<String, PE>();

			for (PE p : pileupElementTracker) {
				String readName = p.getRead().getReadName();

				// if we've never seen this read before, life is good
				if (!filteredPileup.containsKey(readName)) {
					filteredPileup.put(readName, p);
				} else {
					PileupElement existing = filteredPileup.get(readName);

					if (existing.getBase() != p.getBase()) {
						filteredPileup.remove(readName);
					} else {
						if (existing.getQuality() < p.getQuality()) {
							filteredPileup.put(readName, p);
						}
					}
				}
			}

			UnifiedPileupElementTracker<PE> filteredTracker = new UnifiedPileupElementTracker<PE>();
			for (PE filteredElement : filteredPileup.values())
				filteredTracker.add(filteredElement);

			return (AP) createNewPileup(location, filteredTracker);
		}
	}

	/**
	 * Gets a pileup consisting of all those elements passed by a given filter.
	 */
	@SuppressWarnings("unchecked")
	public AP getFilteredPileup(PileupElementFilter filter) {
		if (pileupElementTracker instanceof SamplePileupElementTracker) {
			SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
			SamplePileupElementTracker<PE> filteredTracker = new SamplePileupElementTracker<PE>();

			for (final String sample : tracker.getSamples()) {
				PileupElementTracker<PE> perSampleElements = tracker
						.getElements(sample);
				AbstractPileup<AP, PE> pileup = createNewPileup(
						location, perSampleElements).getFilteredPileup(filter);
				filteredTracker
						.addElements(sample, pileup.pileupElementTracker);
			}

			return (AP) createNewPileup(location, filteredTracker);
		} else {
			UnifiedPileupElementTracker<PE> filteredTracker = new UnifiedPileupElementTracker<PE>();

			for (PE p : pileupElementTracker) {
				if (filter.allow(p))
					filteredTracker.add(p);
			}

			return (AP) createNewPileup(location, filteredTracker);
		}
	}

	/**
	 * Gets a list of the read groups represented in this pileup.
	 */
	@Override
	public Collection<String> getReadGroups() {
		Set<String> readGroups = new HashSet<String>();
		for (PileupElement pileupElement : this)
			readGroups.add(pileupElement.getRead().getReadGroup()
					.getReadGroupId());
		return readGroups;
	}

	@Override
	public Collection<String> getSamples() {
		if (pileupElementTracker instanceof SamplePileupElementTracker) {
			SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
			return new HashSet<String>(tracker.getSamples());
		} else {
			Collection<String> sampleNames = new HashSet<String>();
			for (PileupElement p : this) {
				GaeaSamRecord read = p.getRead();
				String sampleName = read.getReadGroup() != null ? read
						.getReadGroup().getSample() : null;
				sampleNames.add(sampleName);
			}
			return sampleNames;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
    public AP getPileupForSamples(Collection<String> sampleNames) {
        if (pileupElementTracker instanceof SamplePileupElementTracker) {
            SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
            PileupElementTracker<PE> filteredElements = tracker.getElements(sampleNames);
            return filteredElements != null ? (AP) createNewPileup(location, filteredElements) : null;
        } else {
            HashSet<String> hashSampleNames = new HashSet<String>(sampleNames);                                        
            UnifiedPileupElementTracker<PE> filteredTracker = new UnifiedPileupElementTracker<PE>();
            for (PE p : pileupElementTracker) {
                GaeaSamRecord read = p.getRead();
                if (sampleNames != null) {
                    if (read.getReadGroup() != null && hashSampleNames.contains(read.getReadGroup().getSample()))
                        filteredTracker.add(p);
                } else {
                    if (read.getReadGroup() == null || read.getReadGroup().getSample() == null)
                        filteredTracker.add(p);
                }
            }
            return filteredTracker.size() > 0 ? (AP) createNewPileup(location, filteredTracker) : null;
        }
    }

	@Override
	public Map<String, Pileup> getPileupsForSamples(
			Collection<String> sampleNames) {
		Map<String, Pileup> result = new HashMap<String, Pileup>();
		if (pileupElementTracker instanceof SamplePileupElementTracker) {
			SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
			for (String sample : sampleNames) {
				PileupElementTracker<PE> filteredElements = tracker
						.getElements(sample);
				if (filteredElements != null)
					result.put(sample,
							createNewPileup(location, filteredElements));
			}
		} else {
			Map<String, UnifiedPileupElementTracker<PE>> trackerMap = new HashMap<String, UnifiedPileupElementTracker<PE>>();
			for (String sample : sampleNames) { 
				UnifiedPileupElementTracker<PE> filteredTracker = new UnifiedPileupElementTracker<PE>();
				trackerMap.put(sample, filteredTracker);
			}
			for (PE p : pileupElementTracker) { 
				GaeaSamRecord read = p.getRead();
				if (read.getReadGroup() != null) {
					String sample = read.getReadGroup().getSample();
					UnifiedPileupElementTracker<PE> tracker = trackerMap
							.get(sample);
					if (tracker != null) // we only add the pileup the requested samples
						tracker.add(p);
				}
			}
			for (Map.Entry<String, UnifiedPileupElementTracker<PE>> entry : trackerMap
					.entrySet())
				// create the AP for each sample
				result.put(entry.getKey(),
						createNewPileup(location, entry.getValue()));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public AP getPileupForSample(String sampleName) {
		if (pileupElementTracker instanceof SamplePileupElementTracker) {
			SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
			PileupElementTracker<PE> filteredElements = tracker
					.getElements(sampleName);
			return filteredElements != null ? (AP) createNewPileup(location,
					filteredElements) : null;
		} else {
			UnifiedPileupElementTracker<PE> filteredTracker = new UnifiedPileupElementTracker<PE>();
			for (PE p : pileupElementTracker) {
				GaeaSamRecord read = p.getRead();
				if (sampleName != null) {
					if (read.getReadGroup() != null
							&& sampleName.equals(read.getReadGroup()
									.getSample()))
						filteredTracker.add(p);
				} else {
					if (read.getReadGroup() == null
							|| read.getReadGroup().getSample() == null)
						filteredTracker.add(p);
				}
			}
			return filteredTracker.size() > 0 ? (AP) createNewPileup(location,
					filteredTracker) : null;
		}
	}
	
	@Override
	public Iterator<PileupElement> iterator() {
		return new Iterator<PileupElement>() {
			private final Iterator<PE> wrappedIterator = pileupElementTracker
					.iterator();

			public boolean hasNext() {
				return wrappedIterator.hasNext();
			}

			public PileupElement next() {
				return wrappedIterator.next();
			}

			public void remove() {
				throw new UnsupportedOperationException(
						"Cannot remove from a pileup element iterator");
			}
		};
	}

	/**
	 * Simple useful routine to count the number of deletion bases in this
	 * pileup
	 */
	@Override
	public int getNumberOfDeletions() {
		return nDeletions;
	}

	@Override
	public int getNumberOfMappingQualityZeroReads() {
		return nMQ0Reads;
	}

	/**
	 * the number of physical elements in this pileup
	 */
	@Override
	public int getNumberOfElements() {
		return size;
	}

	/**
	 * the number of abstract elements in this pileup
	 */
	@Override
	public int depthOfCoverage() {
		if (abstractSize == -1)
			calculateAbstractSize();
		return abstractSize;
	}

	/**
	 * true if there are 0 elements in the pileup, false otherwise
	 */
	@Override
	public boolean isEmpty() {
		return size == 0;
	}
	
	/**
	 * Get counts of A, C, G, T in order
	 */
	@Override
	public int[] getBaseCounts() {
		int[] counts = new int[4];

		if (pileupElementTracker instanceof SamplePileupElementTracker) {
			SamplePileupElementTracker<PE> tracker = (SamplePileupElementTracker<PE>) pileupElementTracker;
			for (final String sample : tracker.getSamples()) {
				int[] countsBySample = createNewPileup(location,
						tracker.getElements(sample)).getBaseCounts();
				for (int i = 0; i < counts.length; i++)
					counts[i] += countsBySample[i];
			}
		} else {
			for (PileupElement pile : this) {
				// skip deletion sites
				if (!pile.isDeletion()) {
					int index = BaseUtils.simpleBaseToBaseIndex(pile
							.getBase());
					if (index != -1)
						counts[index]++;
				}
			}
		}

		return counts;
	}
	
	@Override
	public GenomeLocation getLocation() {
		return location;
	}


	@Override
	public String toString(Character ref) {
		return String.format("%s %s %c %s %s", getLocation().getContig(),
				getLocation().getStart(), // chromosome name and coordinate
				ref, // reference base
				new String(getBases()), getQualitiesString());
	}

	/**
	 * Returns a list of the reads in this pileup.
	 */
	@Override
	public List<GaeaSamRecord> getReads() {
		List<GaeaSamRecord> reads = new ArrayList<GaeaSamRecord>(
				getNumberOfElements());
		for (PileupElement pile : this) {
			reads.add(pile.getRead());
		}
		return reads;
	}

	@Override
	public int getNumberOfDeletionsAfterThisElement() {
		int count = 0;
		for (PileupElement p : this) {
			if (p.isBeforeDeletionStart())
				count++;
		}
		return count;
	}

	@Override
	public int getNumberOfInsertionsAfterThisElement() {
		int count = 0;
		for (PileupElement p : this) {
			if (p.isBeforeInsertion())
				count++;
		}
		return count;

	}

	/**
	 * Returns a list of the offsets in this pileup. 
	 */
	@Override
	public List<Integer> getOffsets() {
		List<Integer> offsets = new ArrayList<Integer>(getNumberOfElements());
		for (PileupElement pile : this) {
			offsets.add(pile.getOffset());
		}
		return offsets;
	}

	/**
	 * Returns an array of the bases in this pileup.
	 */
	@Override
	public byte[] getBases() {
		byte[] v = new byte[getNumberOfElements()];
		int pos = 0;
		for (PileupElement pile : pileupElementTracker) {
			v[pos++] = pile.getBase();
		}
		return v;
	}

	/**
	 * Returns an array of the qualities in this pileup.
	 */
	@Override
	public byte[] getQualities() {
		byte[] v = new byte[getNumberOfElements()];
		int pos = 0;
		for (PileupElement pile : pileupElementTracker) {
			v[pos++] = pile.getQuality();
		}
		return v;
	}

	/**
	 * Get an array of the mapping qualities
	 */
	@Override
	public byte[] getMappingQualities() {
		byte[] v = new byte[getNumberOfElements()];
		int pos = 0;
		for (PileupElement pile : pileupElementTracker) {
			v[pos++] = (byte) pile.getRead().getMappingQuality();
		}
		return v;
	}

	private String getQualitiesString() {
		byte[] qualities = getQualities();
		StringBuilder qualStr = new StringBuilder();
		for (int qual : qualities) {
			qual = Math.min(qual, 63); // todo: fixme
			char qualChar = (char) (33 + qual); 
			qualStr.append(qualChar);
		}

		return qualStr.toString();
	}

	@Override
	public FragmentCollection<PileupElement> toFragments() {
		return FragmentUtils.create(this);
	}
	
	public boolean hasReads(){
		return size > 0 ;
	}
}
