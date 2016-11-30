package org.bgi.flexlab.gaea.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileManager {
	
	public final static String EOF = "end of file";
	
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
	 
	public static FSDataInputStream getInputStream(Path path, Configuration conf) {
		FileSystem fs = getFileSystem(path, conf);
		try {
			return fs.open(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	public static FSDataOutputStream getOutputStream(Path path, Configuration conf) {
		FileSystem fs = getFileSystem(path, conf);
		try {
			return fs.create(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
}
