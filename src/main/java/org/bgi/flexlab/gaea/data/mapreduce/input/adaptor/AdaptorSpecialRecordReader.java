package org.bgi.flexlab.gaea.data.mapreduce.input.adaptor;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class AdaptorSpecialRecordReader extends AdaptorRecordReader {
	@Override
	public boolean nextKeyValue() throws IOException {
		Text temp = new Text();
		while (pos < end) {
			int newSize = in.readLine(temp, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				return false;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				String line = temp.toString();
				String[] splitLines = line.split("\t");
				if (splitLines[0].startsWith("#")) {
					continue;
				}
				String tempkey, tempvalue;
				splitLines[0] += "/1";
				int index = splitLines[0].lastIndexOf("/");

				tempkey = splitLines[0].substring(0, index).trim();
				tempvalue = splitLines[0].substring(index + 1).trim();

				key.set(tempkey);
				value.set(tempvalue);
				return true;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		return false;
	}
}
