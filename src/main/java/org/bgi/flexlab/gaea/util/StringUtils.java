package org.bgi.flexlab.gaea.util;

import java.util.ArrayList;
import java.util.List;

import org.bgi.flexlab.gaea.data.exception.UserException;

public class StringUtils {
	public static String join(String[] str, String seperator) {
		boolean first = true;

		StringBuilder sb = new StringBuilder();
		for (String s : str) {
			if (first) {
				sb.append(s);
				first = false;
			} else {
				sb.append(seperator + s);
			}
		}
		return sb.toString();
	}

	public static List<Integer> getWordStarts(String line) {
		if (line == null)
			throw new UserException("line is null");
		List<Integer> starts = new ArrayList<Integer>();
		int stop = line.length();
		for (int i = 1; i < stop; i++)
			if (Character.isWhitespace(line.charAt(i - 1)))
				if (!Character.isWhitespace(line.charAt(i)))
					starts.add(i);
		return starts;
	}

	public static String[] splitFixedWidth(String line) {
		List<Integer> starts = getWordStarts(line);
		
		return splitFixedWidth(line,starts);
	}
	
	public static String[] splitFixedWidth(String line,List<Integer> starts) {
		if (line == null)
			throw new UserException("line is null");
		if (starts == null)
			throw new UserException("columnStarts is null");
		int startCount = starts.size();
		String[] row = new String[startCount + 1];
		if (startCount == 0) {
			row[0] = line.trim();
		} else {
			row[0] = line.substring(0, starts.get(0)).trim();
			for (int i = 1; i < startCount; i++)
				row[i] = line.substring(starts.get(i - 1), starts.get(i)).trim();
			row[startCount] = line.substring(starts.get(startCount - 1)).trim();
		}
		return row;
	}
}
