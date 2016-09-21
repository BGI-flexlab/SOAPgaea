package org.bgi.flexlab.gaea.data.mapreduce.input.cram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.io.CountingInputStream;
import htsjdk.samtools.cram.structure.Container;

public class ChromosomeIndex {
	private String cramFileName;
	private SAMFileHeader samFileHeader;
	private String cramIndexFileName = null;
	
	private HashMap<Integer,ChromosomeIndexStructure> chromosomeIndexMap = null; 
	
	class ChromosomeIndexStructure{
		private int sequenceId;
		private long start;
		private long end;
		
		public ChromosomeIndexStructure(int sid,long s,long e){
			sequenceId = sid;
			start = s;
			end = e;
		}
		
		public ChromosomeIndexStructure(String line){
			String[] str = line.split("\t");
			sequenceId = Integer.parseInt(str[0]);
			start = Long.parseLong(str[1]);
			end = Long.parseLong(str[2]);
		}
		
		public int getSeqId(){
			return sequenceId;
		}
		
		public long getStart(){
			return this.start;
		}
		
		public long getEnd(){
			return this.end;
		}
		
		public String toString(){
			return sequenceId+"\t"+start+"\t"+end;
		}
	}
	
	public ChromosomeIndex(String cram){
		this.cramFileName = cram;
		cramIndexFileName = this.cramFileName+".crai";
	}
	
	public ChromosomeIndex(String cram_,String index_){
		this.cramFileName = cram_;
		this.cramIndexFileName = index_;
	}
	
	public void setHeader(SAMFileHeader header){
		samFileHeader = header;
	}
	
	public ArrayList<ChromosomeIndexStructure> indexForChromosome(Path p){
		CountingInputStream is = null;
		try {
			is = new CountingInputStream(p.getFileSystem(new Configuration()).open(p));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			samFileHeader = CramIO.readCramHeader(is).getSamFileHeader();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		long offset = is.getCount();
		Container c = null;
		int prevSeqID = Integer.MIN_VALUE;
		long start = -1,end = -1;
		
		ArrayList<ChromosomeIndexStructure> chrIndex = new ArrayList<ChromosomeIndexStructure>();
		
		try {
			while((c = CramIO.readContainer(is) ) != null){
				c.offset = offset;
				if(c.sequenceId != prevSeqID){
					if(prevSeqID != Integer.MIN_VALUE){
						end = c.offset - 1; 
						chrIndex.add(new ChromosomeIndexStructure(prevSeqID,start,end));
					}
					start = offset;
					prevSeqID = c.sequenceId;
				}
				offset = is.getCount();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		chrIndex.add(new ChromosomeIndexStructure(prevSeqID,start,end));
		
		return chrIndex;
	}
	
	public void writeIndex(ArrayList<ChromosomeIndexStructure> chrIndex){
		Path p = new Path(cramIndexFileName);
		Configuration conf = new Configuration();
		FSDataOutputStream output = null;
		try {
			output = p.getFileSystem(conf).create(p);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(ChromosomeIndexStructure chrindex : chrIndex){
			try {
				output.write(chrindex.toString().getBytes());
				output.write("\n".getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void loadChromosomeIndex(){
		chromosomeIndexMap = new HashMap<Integer,ChromosomeIndexStructure>();
		Path p = new Path(cramIndexFileName);
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = p.getFileSystem(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			if(!fs.exists(p)){
				writeIndex(indexForChromosome(new Path(cramFileName)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
			
		try {
			FSDataInputStream reader = p.getFileSystem(conf).open(p);
			LineReader lineReader = new LineReader(reader, conf);
			Text line = new Text();
			while(lineReader.readLine(line) > 0) {
				if(line.getLength() == 0)continue;
				String[] str = line.toString().split("\t");
				chromosomeIndexMap.put(Integer.parseInt(str[0]), new ChromosomeIndexStructure(line.toString()));
			}
			
			lineReader.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public long getStart(String chrName){
		if(chromosomeIndexMap == null)
			loadChromosomeIndex();
		int seqId = samFileHeader.getSequenceIndex(chrName);
		if(chromosomeIndexMap.containsKey(seqId))
			return chromosomeIndexMap.get(seqId).getStart();
		else
			return -1;
	}
	
	public long getEnd(String chrName){
		if(chromosomeIndexMap == null)
			loadChromosomeIndex();
		int seqId = samFileHeader.getSequenceIndex(chrName);
		if(chromosomeIndexMap.containsKey(seqId))
			return chromosomeIndexMap.get(seqId).getEnd();
		else
			return -1;
	}
}

