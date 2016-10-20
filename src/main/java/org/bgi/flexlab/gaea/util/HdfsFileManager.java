package org.bgi.flexlab.gaea.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileManager {
	
	public static  FileSystem getFileSystem(Path path,Configuration conf) {
  		FileSystem fs=null;
  		try {
			fs=path.getFileSystem(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 		return fs;
	}
	 
	public static FSDataInputStream getInputStream(Path path, Configuration conf) throws IOException {
		FileSystem fs = getFileSystem(path, conf);
		return fs.open(path);
	}
	
	public static FSDataOutputStream getOutputStream(Path path, Configuration conf) throws IOException {
		FileSystem fs = getFileSystem(path, conf);
		return fs.create(path);
	}
	
}
