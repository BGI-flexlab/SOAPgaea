package org.bgi.flexlab.gaea.structure.header;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class GaeaVCFHeader implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4160938012339601002L;

	
	public String writeHeaderToHDFS(String outputPath){
		String uri = writeHeader(outputPath);
		writeToHDFS(new Path(uri.toString()));
		return uri;
	}
	
	public boolean writeToHDFS(Path path){
		ObjectOutputStream ostream = null;
		try {
			FileSystem fs = path.getFileSystem(new Configuration());
			ostream = new ObjectOutputStream(fs.create(path));
			ostream.writeObject(this);
			ostream.close();
			fs.close();
		} catch (IOException e) {
			ostream = null;
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public String writeHeader(String outputPath){
		StringBuilder uri = new StringBuilder();
		uri.append(outputPath);
		uri.append(System.getProperty("file.separator"));
		uri.append("vcfHeader");
		uri.append(System.getProperty("file.separator"));
		uri.append("VcfHeader.obj");
		return uri.toString();
	}
	

	public GaeaVCFHeader readFromFile(String file) throws IOException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream stream = fs.open(path);
		ObjectInputStream ostream = new ObjectInputStream(stream);
		GaeaVCFHeader vcfHeader = (GaeaVCFHeader) ostream.readObject();
		ostream.close();
		return vcfHeader;
	}

	public void loadVcfHeader(String output){
		GaeaVCFHeader vcfHeaderTmp = initializeHeader();
		try {
			if(output == null){//distribute cache
				vcfHeaderTmp = readFromFile("VcfHeaderObj");
			} else {//no distribute cache
				String uri = writeHeader(output);
				vcfHeaderTmp = readFromFile(uri.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		copy(vcfHeaderTmp);
	}

	abstract void copy(GaeaVCFHeader header);
	
	abstract GaeaVCFHeader initializeHeader();
}
