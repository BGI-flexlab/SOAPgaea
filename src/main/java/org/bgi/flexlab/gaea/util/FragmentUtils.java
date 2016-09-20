package org.bgi.flexlab.gaea.util;

import htsjdk.samtools.SAMRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;
import org.bgi.flexlab.gaea.exception.UnsortedException;

public class FragmentUtils {

	private FragmentUtils() {
	} // private constructor

	/**
	 * A getter function that takes an Object of type T and returns its
	 * associated SAMRecord.
	 */
	public interface ReadGetter<T> {
		public GaeaSamRecord get(T object);
	}

	/** Identify getter for SAMRecords themselves */
	private final static ReadGetter<GaeaSamRecord> SamRecordGetter = new ReadGetter<GaeaSamRecord>() {
		@Override
		public GaeaSamRecord get(final GaeaSamRecord object) {
			return object;
		}
	};

	/** Gets the SAMRecord in a PileupElement */
	private final static ReadGetter<PileupElement> PileupElementGetter = new ReadGetter<PileupElement>() {
		@Override
		public GaeaSamRecord get(final PileupElement object) {
			return object.getRead();
		}
	};

	/**
	 * Generic algorithm that takes an iterable over T objects, a getter routine
	 * to extract the reads in T, and returns a FragmentCollection that contains
	 * the T objects whose underlying reads either overlap (or not) with their
	 * mate pairs.
	 */
	@SuppressWarnings("unchecked")
	private final static <T> FragmentCollection<T> create(
			Iterable<T> readContainingObjects, int nElements,
			ReadGetter<T> getter) {
		Collection<T> singletons = null;
		Collection<List<T>> overlapping = null;
		Map<String, T> nameMap = null;

		int lastStart = -1;

		// build an initial map, grabbing all of the multi-read fragments
		for (final T p : readContainingObjects) {
			final SAMRecord read = getter.get(p);

			if (read.getAlignmentStart() < lastStart) {
				throw new UnsortedException(read.getReadName(),
						read.getAlignmentStart(), lastStart);
			}
			lastStart = read.getAlignmentStart();

			final int mateStart = read.getMateAlignmentStart();
			if (mateStart == 0 || mateStart > read.getAlignmentEnd()) {
				// if we know that this read won't overlap its mate, or doesn't
				// have one, jump out early
				if (singletons == null)
					singletons = new ArrayList<T>(nElements); // lazy init
				singletons.add(p);
			} else {
				// the read might overlap it's mate, or is the rightmost read of
				// a pair
				final String readName = read.getReadName();
				final T pe1 = nameMap == null ? null : nameMap.get(readName);
				if (pe1 != null) {
					// assumes we have at most 2 reads per fragment
					if (overlapping == null)
						overlapping = new ArrayList<List<T>>(); // lazy init
					overlapping.add(Arrays.asList(pe1, p));
					nameMap.remove(readName);
				} else {
					if (nameMap == null)
						nameMap = new HashMap<String, T>(nElements); // lazy init
					nameMap.put(readName, p);
				}
			}
		}

		// add all of the reads that are potentially overlapping but whose mate
		// never showed
		// up to the oneReadPile
		if (nameMap != null && !nameMap.isEmpty()) {
			if (singletons == null)
				singletons = nameMap.values();
			else
				singletons.addAll(nameMap.values());
		}

		return new FragmentCollection<T>(singletons, overlapping);
	}

	public final static FragmentCollection<PileupElement> create(Pileup pileup) {
		return create(pileup, pileup.getNumberOfElements(), PileupElementGetter);
	}

	public final static FragmentCollection<GaeaSamRecord> create(
			List<GaeaSamRecord> reads) {
		return create(reads, reads.size(), SamRecordGetter);
	}
}
