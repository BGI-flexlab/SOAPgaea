package org.bgi.flexlab.gaea.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class FileIterator {
	private Configuration conf = new Configuration();
	private FileSystem fs;
	private LineReader reader = null;
	private Text value = null;
	private Path path;

	public FileIterator(String file) throws IOException {
		this.path = new Path(file);
		read();
	}

	public FileIterator(Path p) throws IOException {
		this.path = p;
		read();
	}

	@SuppressWarnings("deprecation")
	private void read() throws IOException {
		fs = getFileSystem(path, conf);
		FSDataInputStream in;
		if (fs.getLength(path) == 0)
			return;
		in = fs.open(path);
		reader = new LineReader(in, conf);
	}

	public boolean hasNext() {
		if (value != null)
			return true;
		if (reader == null)
			return false;
		value = new Text();
		try {
			if (reader.readLine(value) > 0 && value.getLength() != 0) {
				return true;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return false;
	}

	public Text next() {
		if (value == null) {
			hasNext();
		}
		Text current = value;
		value = null;
		return current;
	}

	private static FileSystem getFileSystem(Path path, Configuration conf) {
		FileSystem fs = null;
		try {
			fs = path.getFileSystem(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fs;
	}

	public void close() {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
