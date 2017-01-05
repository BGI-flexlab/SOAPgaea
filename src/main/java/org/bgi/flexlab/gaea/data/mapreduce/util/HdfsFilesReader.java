package org.bgi.flexlab.gaea.data.mapreduce.util;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.exception.FileNotExistException;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader.HeaderPathFilter;
import org.bgi.flexlab.gaea.util.GaeaFilesReader;

public class HdfsFilesReader extends GaeaFilesReader{
	private Configuration conf;
	private LineReader lineReader = null;
	private ArrayList<Path> files = null;
	private FileSystem fs = null;
	
	public HdfsFilesReader(){
		this(new Configuration());
	}
	
	public HdfsFilesReader(Configuration conf){
		this.conf = conf;
		files = new ArrayList<Path>();
		currentFileIndex = 0;
		currentLine = null;
	}

	@Override
	public void traversal(String path){
		traversal(path,new HeaderPathFilter());
	}
	
	public void traversal(String path,PathFilter pathFilter) {
		Path p = new Path(path);
		
		fs = HdfsFileManager.getFileSystem(p, conf);
		FileStatus status = null;
		try {
			status = fs.getFileStatus(p);
		} catch (IOException e2) {
			throw new FileNotExistException(p.getName());
		}
		
		if(status.isFile()){
			if(!filter(p.getName()))
				files.add(p);
		}else{
			FileStatus[] stats = null;
			try{
				stats = fs.listStatus(p,pathFilter);
			}catch(IOException e){
				throw new RuntimeException(e.toString());
			}

			for (FileStatus file : stats) {
				if(!file.isFile()){
					traversal(file.toString(),pathFilter);
				}else{
					if(!filter(file.getPath().getName()))
						files.add(file.getPath());
				}
			}
		}
		
		if(size() == 0)
			return;
		FSDataInputStream currInput;
		try {
			currInput = fs.open(files.get(0));
			lineReader = new LineReader(currInput,conf);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	@Override
	public boolean hasNext() {
		if(lineReader != null){
			Text line = new Text();
			try {
				if(lineReader.readLine(line) > 0){
					currentLine = line.toString();
					return true;
				}else{
					lineReader.close();
					currentFileIndex++;
					
					if(currentFileIndex < size()){
						FSDataInputStream currInput = fs.open(files.get(currentFileIndex));
						lineReader = new LineReader(currInput,conf);
						if(lineReader.readLine(line) > 0){
							currentLine = line.toString();
							return true;
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
		}
		
		currentLine = null;
		return false;
	}
	
	public void delete(){
		for(Path path : files){
			fs = HdfsFileManager.getFileSystem(path, conf);
			try {
				fs.deleteOnExit(path);
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
		}
	}

	@Override
	public void clear() {
		if(lineReader != null){
			try {
				lineReader.close();
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
		}
		files.clear();
		currentLine = null;
		currentFileIndex = 0;
	}

	@Override
	protected int size() {
		return files.size();
	}
}
