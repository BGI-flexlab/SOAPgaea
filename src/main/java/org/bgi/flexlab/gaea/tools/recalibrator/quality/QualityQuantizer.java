package org.bgi.flexlab.gaea.tools.recalibrator.quality;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.util.QualityUtils;

public class QualityQuantizer {
	private List<Long> observationNumbers = null;
	private final int level;
	private final int minimumQuality;

	public QualityQuantizer(final List<Long> obser, final int level, final int minQuality) {
		this.observationNumbers = obser;
		this.level = level;
		this.minimumQuality = minQuality;

		if (Collections.min(obser) < 0)
			throw new UserException("Quality score has negative values!");
		if (level < 0)
			throw new UserException("quality level must be >= 0!");
		if (minQuality < 0)
			throw new UserException("minimum quality must be >= 0!");
	}

	private TreeSet<QualityInterval> quantize() {
		final TreeSet<QualityInterval> intervals = new TreeSet<QualityInterval>();
		for (int qStart = 0; qStart < observationNumbers.size(); qStart++) {
			final long nObs = observationNumbers.get(qStart);
			final double errorRate = QualityUtils.qualityToErrorProbability((byte) qStart);
			final double nErrors = nObs * errorRate;
			final QualityInterval qi = new QualityInterval(qStart, 0, nObs, (int) Math.floor(nErrors));
			intervals.add(qi);
		}

		while (intervals.size() > level) {
			mergeLowestPenaltyIntervals(intervals);
		}

		return intervals;
	}

	private void mergeLowestPenaltyIntervals(final TreeSet<QualityInterval> intervals) {
		final Iterator<QualityInterval> iter = intervals.iterator();
		final Iterator<QualityInterval> iterator = intervals.iterator();
		iterator.next();

		QualityInterval minMerge = null;
		while (iterator.hasNext()) {
			final QualityInterval left = iter.next();
			final QualityInterval right = iterator.next();
			if (left.canMerge(right)) {
				final QualityInterval merged = left.merge(right);
				if (minMerge == null || (merged.getPenalty(minimumQuality) < minMerge.getPenalty(minimumQuality))) {
					minMerge = merged;
				}
			}
		}

		intervals.removeAll(minMerge.subIntervals);
		intervals.add(minMerge);
	}

	public List<Byte> getIntervals() {
		TreeSet<QualityInterval> qualityIntervals = quantize();
		final List<Byte> map = new ArrayList<Byte>(observationNumbers.size());
		map.addAll(Collections.nCopies(observationNumbers.size(), Byte.MIN_VALUE));
		for (final QualityInterval interval : qualityIntervals) {
			for (int q = interval.qStart; q <= interval.qEnd; q++) {
				map.set(q, interval.getQual());
			}
		}

		if (Collections.min(map) == Byte.MIN_VALUE)
			throw new UserException("quantized quality score map contains an un-initialized value!");

		return map;
	}
}
