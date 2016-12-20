package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.covariate;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibratorOptions;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.ReadCovariates;
import org.bgi.flexlab.gaea.util.QualityUtils;

public class QualityCovariate implements RequiredCovariate {

	@Override
	public void initialize(BaseRecalibratorOptions option) {}

	@Override
	public void recordValues(final GaeaSamRecord read, final ReadCovariates values) {
		final byte[] baseQualities = read.getBaseQualities();
		final byte[] baseInsertionQualities = read.getBaseInsertionQualities();
		final byte[] baseDeletionQualities = read.getBaseDeletionQualities();

		for (int i = 0; i < baseQualities.length; i++) {
			values.addCovariate(baseQualities[i], baseInsertionQualities[i], baseDeletionQualities[i], i);
		}
	}

	@Override
	public final Object getValue(final String str) {
		return Byte.parseByte(str);
	}

	@Override
	public String formatKey(final int key) {
		return String.format("%d", key);
	}

	@Override
	public int keyFromValue(final Object value) {
		return (value instanceof String) ? (int) Byte.parseByte((String) value) : (int) (Byte) value;
	}

	@Override
	public int maximumKeyValue() {
		return QualityUtils.MAXIMUM_USABLE_QUALITY_SCORE;
	}
}
