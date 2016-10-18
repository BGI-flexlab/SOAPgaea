package org.bgi.flexlab.gaea.data.structure.vcf.index;
import java.io.IOException;
import java.io.OutputStream;


public class VCFIndexHeader {
	private final Index.TYPE type=Index.TYPE.H;
	
	private String version;
	private int headerSize;
	private int maxBlockSize;
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public int getHeaderSize() {
		return headerSize;
	}
	public void setHeaderSize(int headerSize) {
		this.headerSize = headerSize;
	}
	public int getMaxBlockSize() {
		return maxBlockSize;
	}
	public void setMaxBlockSize(int maxBlockSize) {
		this.maxBlockSize = maxBlockSize;
	}

	public void write(OutputStream out) {
		validHeader();
		StringBuilder sb=new StringBuilder();
		sb.append(type.toString());sb.append("\t");
		sb.append(version);sb.append("\t");
		sb.append(headerSize);sb.append("\t");
		sb.append(maxBlockSize);sb.append("\n");
		try {
			out.write(sb.toString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void read(String line) {
		if(line.charAt(0)!='H') {
			System.err.println(line+"\tis not header index");
			System.exit(-1);
		}
		String[] header=line.split("\t");
		if(header.length!=4) {
			System.err.println(line+":split length is no 4!");
			System.exit(-1);
		}
		version=header[1];
		headerSize=Integer.parseInt(header[2]);
		maxBlockSize=Integer.parseInt(header[3]);
		validHeader();
	}
	
	public void validHeader() {
		if(version==null||version.equals("")) {
			System.err.println("HEADER:the version is not exsit!");
			System.exit(-1);
		}
		if(headerSize==0) {
			System.err.println("HEADER:the header size is 0!");
			System.exit(-1);
		}
		if(maxBlockSize==0) {
			System.err.println("HEADER:the max block size is 0!");
			System.exit(-1);
		}
	}
}
