package org.bgi.flexlab.gaea.tools.recalibrator.report;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang.math.NumberUtils;
import org.bgi.flexlab.gaea.util.StandardDataType;

public class RecalibratorReportColumn {
	private boolean isRightAlignment = false;
	private final StandardDataType dataType;
	private final String format;
	private int maxWidth;
	private String name;

	public RecalibratorReportColumn(final String columnName, final String format) {
		this.name = columnName;
		this.maxWidth = columnName.length();
		if (format.equals("")) {
			this.format = "%s";
			this.dataType = StandardDataType.Unknown;
		} else {
			this.format = format;
			this.dataType = StandardDataType.fromFormatString(format);
		}
	}

	public StandardDataType getDataType() {
		return dataType;
	}

	public void updateFormatting(final Object value) {
		if (value != null) {
			final String formatted = formatValue(value);
			if (formatted.length() > 0) {
				updateMaxWidth(formatted);
				updateFormat(formatted);
			}
		}
	}

	private String formatValue(final Object obj) {
		String value;
		if (obj == null) {
			value = "null";
		} else if (dataType.equals(StandardDataType.Unknown) && (obj instanceof Double || obj instanceof Float)) {
			value = String.format("%.8f", obj);
		} else
			value = String.format(format, obj);

		return value;
	}

	private void updateMaxWidth(final String formatted) {
		maxWidth = Math.max(formatted.length(), maxWidth);
	}

	private static final Collection<String> RIGHT_ALIGN_SETS = Arrays.asList("null", "NA",
			String.valueOf(Double.POSITIVE_INFINITY), String.valueOf(Double.NEGATIVE_INFINITY),
			String.valueOf(Double.NaN));

	protected static boolean isRightAlign(final String value) {
		return value == null || RIGHT_ALIGN_SETS.contains(value) || NumberUtils.isNumber(value.trim());
	}

	private void updateFormat(final String formatted) {
		if (!this.isRightAlignment)
			isRightAlignment = isRightAlign(formatted) ? true : false;
	}

	public String getColumnName() {
		return this.name;
	}

	public String getFormat() {
		return dataType.equals(StandardDataType.Unknown) ? "%s" : format;
	}

	public String getColumnNameFormat() {
		return "%-" + maxWidth + "s";
	}

	public String getColumnValueFormat() {
		if (isRightAlignment)
			return "%" + maxWidth + "s";
		else
			return "%s-" + maxWidth + "s";
	}
}
