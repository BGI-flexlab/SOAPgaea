package org.bgi.flexlab.gaea.util;

public abstract class GaeaFilesReader{
	protected int currentFileIndex = 0;
	protected String currentLine = null;
	
	/**
	 * get all file for input path
	 */
	public abstract void traversal(String path);
	
	/**
	 * has next line can be read?
	 */
	public abstract boolean hasNext();
	
	protected abstract int size();
	
	protected boolean filter(String name){
		if(name.startsWith("_"))
			return true;
		return false;
	}
	
	/**
	 * return next line
	 */
	public String next(){
		return currentLine;
	}
	
	/**
	 * clear all data
	 */
	public abstract void clear();
}
