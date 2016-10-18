package org.bgi.flexlab.gaea.data.structure.vcf.index;
import java.io.IOException;
import java.io.OutputStream;


public class VCFBlock {
	private final Index.TYPE type=Index.TYPE.B;
	private long position;//seek offset
	private int chr;//chr
	private int start;//the start in reference
	private int end;//the end in reference;
	private int blockSize;//VCF record Number
	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}
	public int getChr() {
		return chr;
	}
	public void setChr(int chr) {
		this.chr = chr;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	public int getBlockSize() {
		return blockSize;
	}
	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}
	
	public void write(OutputStream out)
	{
		StringBuilder sb=new StringBuilder();
		sb.append(type.toString());sb.append("\t");
		sb.append(position);sb.append("\t");
		sb.append(chr);sb.append("\t");
		sb.append(start);sb.append("\t");
		sb.append(end);sb.append("\t");
		sb.append(blockSize);sb.append("\n");
		try {
			out.write(sb.toString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void read(String line)
	{
		if(line.charAt(0)!='B')
		{
			System.err.println(line+"\tis not block index!");
			System.exit(-1);
		}
		
		String[] block=line.split("\t");
		if(block.length!=6)
		{
			System.err.println("illegal block index:"+line);
			System.exit(-1);
		}
		this.position=Long.parseLong(block[1]);
		this.chr=Integer.parseInt(block[2]);
		this.start=Integer.parseInt(block[3]);
		this.end=Integer.parseInt(block[4]);
		this.blockSize=Integer.parseInt(block[5]);
		
	}
}
