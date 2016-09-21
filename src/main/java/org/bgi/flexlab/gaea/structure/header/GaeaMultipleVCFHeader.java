package org.bgi.flexlab.gaea.structure.header;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GaeaMultipleVCFHeader extends  GaeaVCFHeader implements Serializable{
	/**
	 * serial ID
	 */
	private static final long serialVersionUID = -5677604795673775528L;

	/**
	 * fileName 2 ID
	 */
	private Map<String, Integer> fileName2ID = new ConcurrentHashMap<String, Integer>();
	
	/**
	 * ID to header
	 */
	private Map<Integer, GaeaSingleVCFHeader> ID2SignelVcfHeader = new ConcurrentHashMap<Integer, GaeaSingleVCFHeader>();
	
	/**
	 * global ID
	 */
	private int id = 0;
	
	/**
	 * get vcf header
	 * @param id
	 * @return header String
	 */
	public String getVcfHeader(int id) {
		return ID2SignelVcfHeader.get(id).getHeaderInfoWithSample(null);
	}
	
	/**
	 * get vcf header lines
	 * @param id
	 * @return
	 */
	public ArrayList<String> getVcfHeaderLines(int id) {
		ArrayList<String> headerLines = new ArrayList<String>();
		for(String line : ID2SignelVcfHeader.get(id).getHeaderInfoStringLines(null)){
			headerLines.add(line);
		}
		return headerLines;
	}
	
	/**
	 * get sample number of id file.
	 * @param id
	 * @return
	 */
	public int getSampleNum(int id) {
		return ID2SignelVcfHeader.get(id).getSampleNames().size();
	}
	
	/**
	 * get samples string of id file
	 * @param id
	 * @return
	 */
	public List<String> getSampleNames(int id) {
		return ID2SignelVcfHeader.get(id).getSampleNames();
	}
	
	/**
	 * get id from fileName, this function is for multi-vcf reader
	 * @param filePathName
	 * @return
	 */
	public int getId(String filePathName) {
		//filePathName = formatFilePath(filePathName);
		//System.err.println("getID:" + filePathName);
		if(fileName2ID.containsKey(filePathName)) {
			return fileName2ID.get(filePathName);
		} else {
			throw new RuntimeException("this file is not in inputs!");
		}
	}
	
	public String getFile(int id) {
		for(String file : fileName2ID.keySet()) {
			if(id == fileName2ID.get(file)){
				return file;
			}
		}
		throw new RuntimeException("no such id in VCFHeader!");
	}
	
	/**
	 * read single vcf file
	 * @param vcf
	 * @param conf
	 * @throws IOException
	 */
	private void readVcfHeader(Path vcf, Configuration conf) throws IOException {
		GaeaSingleVCFHeader singleVcfHeader = new GaeaSingleVCFHeader();
		singleVcfHeader.parseHeader(vcf, null, conf);
		ID2SignelVcfHeader.put(id, singleVcfHeader);
		
		//String filePathName = formatFilePath(vcf.toString());
		fileName2ID.put(vcf.toString(), id);
		
		id++;
	}
	
	@SuppressWarnings("unused")
	private String formatFilePath(String filePathName) {
		if(filePathName.startsWith("file:///")) {
			filePathName = filePathName.substring(7);
		} else {
			if(filePathName.startsWith("file:/")) {
				filePathName = filePathName.substring(5);
			}
		}
		return filePathName.trim();
	}
	
	public void mergeHeader(Path inputPath, String output, Configuration conf, boolean distributeCacheHeader) {
		try {
			FileSystem fs = inputPath.getFileSystem(conf);
			fs = inputPath.getFileSystem(conf);
			if (!fs.exists(inputPath)) {
				System.out.println("Input File Path is not exist! Please check input var.");
				System.exit(-1);
			}
			if (fs.isFile(inputPath)) {
				if(validPath(inputPath, fs)){
					readVcfHeader(inputPath, conf);	
				}	
			}else {		
				FileStatus stats[]=fs.listStatus(inputPath);
				
				for (FileStatus file : stats) {
					Path filePath = file.getPath();
					mergeHeader(filePath, output, conf, distributeCacheHeader);
				}
			}
			fs.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		conf.set("output", output);
		if(distributeCacheHeader){
			distributeCacheVcfHeader(output, conf);
		} else {
			writeHeaderToHDFS(output);
		}
	}
	
	private boolean validPath(Path inputPath, FileSystem fs) throws IOException{
		return (!inputPath.getName().startsWith("_")) && (fs.getFileStatus(inputPath).getLen() != 0);
	}
	
	public boolean distributeCacheVcfHeader(String outputPath, Configuration conf) {
		String output = writeHeaderToHDFS(outputPath);
		try {
			DistributedCache.createSymlink(conf);
			DistributedCache.addCacheFile(new URI(output + "#VcfHeaderObj"), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	@Override
	public void copy(GaeaVCFHeader header){
		GaeaMultipleVCFHeader multiHeader = (GaeaMultipleVCFHeader) header;
		fileName2ID = multiHeader.fileName2ID;
		ID2SignelVcfHeader = multiHeader.ID2SignelVcfHeader;
		id = multiHeader.id;
	}
	
	@Override
	public GaeaVCFHeader initializeHeader(){
		return new GaeaMultipleVCFHeader();
	}
	
	public int getFileNum() {
		return fileName2ID.size();
	}

	public Map<String, Integer> getFileName2ID() {
		return fileName2ID;
	}

	public Map<Integer, GaeaSingleVCFHeader> getID2SignelVcfHeader() {
		return ID2SignelVcfHeader;
	}
}
