package org.bgi.flexlab.gaea.data.mapreduce.input.header;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LineReader;

public class HdfsLineReader extends LineReader{

	public HdfsLineReader(InputStream in, Configuration conf)
			throws IOException {
		super(in, conf);
	}
}
