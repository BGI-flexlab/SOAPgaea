package org.bgi.flexlab.gaea.data.mapreduce.input.header;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import htsjdk.samtools.util.LineReader;

public class HdfsHeaderLineReader implements LineReader{
	private int lineNumber;
	private String currentLine;
	private HdfsLineReader lineReader = null;
	
	public HdfsHeaderLineReader(Path path, Configuration conf) throws IOException{
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream FSinput = fs.open(path);
		lineReader = new HdfsLineReader(FSinput,conf);
	}

	@Override
	public int getLineNumber() {
		return lineNumber;
	}

	@Override
	public int peek() {
		Text tmp = new Text();
		try {
			if(lineReader.readLine(tmp) == 0)
				return -1;
			else
				lineNumber++;
		} catch (IOException e) {
			e.printStackTrace();
		}
		currentLine = tmp.toString();
		return currentLine.charAt(0);
	}

	@Override
	public String readLine() {
		return currentLine;
	}

	@Override
	public void close() {
		try {
			lineReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
