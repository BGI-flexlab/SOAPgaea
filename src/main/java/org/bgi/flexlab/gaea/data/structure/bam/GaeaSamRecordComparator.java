package org.bgi.flexlab.gaea.data.structure.bam;

import java.util.Comparator;

public class GaeaSamRecordComparator implements Comparator<GaeaSamRecord> {

	@Override
	public int compare(GaeaSamRecord s1, GaeaSamRecord s2) {
		return s1.getAlignmentStart() - s2.getAlignmentStart();
	}
}
