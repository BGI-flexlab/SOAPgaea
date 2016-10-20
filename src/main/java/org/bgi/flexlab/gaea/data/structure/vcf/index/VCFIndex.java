package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.index.tabix.TabixIndexCreator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;



public class VCFIndex implements Index<VCFIndexHeader, IndexReferenceDictionary, VCFBlock> {

	private static final String METADATA_INDICATOR = "#";
	private static final String VERSION_FORMAT_STRING = "fileformat";
	private VCFIndexHeader headerIndex = null;
	private IndexReferenceDictionary dictionary = null;
	private HashMap<Integer,ArrayList<VCFBlock>> blocks = null;
	private int blockSize;
	private Configuration conf;
	
	public void initialize(Configuration conf,int blockSize) {
		this.conf=conf;
		this.blockSize=blockSize;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List getBlock(String chr, int start, int end) {
		// TODO Auto-generated method stub
		if(!dictionary.containsKey(chr))
		{
			System.err.println("dictionary has not that chr!");
			return null;
		}
		if(blocks==null){
			System.err.println(chr+":"+start);
			return null;
		}
		
		int chrIndex=dictionary.get(chr);
		ArrayList<VCFBlock> b=new ArrayList<VCFBlock>();
		if(!blocks.containsKey(chrIndex))
			return null;
		ArrayList<VCFBlock> chrblock=blocks.get(chrIndex);
		if(chrblock==null||chrblock.size()==0)
			return null;
		for(VCFBlock block:chrblock)
		{
			if(block.getEnd()<start)
				continue;
			else if(block.getStart()>end)
				break;
			else
				b.add(block);
			
		}
				if(b.size()>0)
					return b;
		return null;
	}

	@Override
	public boolean containsChromosome(String chr) {
		// TODO Auto-generated method stub
		return dictionary.containsKey(chr);
	}

	@Override
	public void read(InputStream stream) {
		// TODO Auto-generated method stub
		init();
		try {
			LineReader lineReader = new LineReader(stream, conf);
			Text line=new Text();
			String tempString=null;
			while((lineReader.readLine(line))>0){
				tempString=line.toString();
				if(tempString.charAt(0)=='H')
					headerIndex.read(tempString);
				else if(tempString.charAt(0)=='D')
					dictionary.read(tempString);
				else if(tempString.charAt(0)=='B'){
					VCFBlock block=new VCFBlock();
					block.read(tempString);
					if(!blocks.containsKey(block.getChr()))
						blocks.put(block.getChr(), new ArrayList<VCFBlock>());
					blocks.get(block.getChr()).add(block);
				}else{
					System.err.println("unknow index type!");
					System.exit(-1);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(OutputStream stream) {
		// TODO Auto-generated method stub
		headerIndex.write(stream);
		dictionary.write(stream);
		for(Integer key:blocks.keySet())
		{
			for(VCFBlock block:blocks.get(key))
				block.write(stream);
		}
	}

	@Override
	public VCFIndexHeader header() {
		// TODO Auto-generated method stub
		return headerIndex;
	}

	@Override
	public IndexReferenceDictionary dictionary() {
		// TODO Auto-generated method stub
		return dictionary;
	}

	@Override
	public HashMap<Integer,ArrayList<VCFBlock>> blocks() {
		// TODO Auto-generated method stub
		return blocks;
	}


	@Override
	public void create(InputStream stream) {
		// TODO Auto-generated method stub
		if(blockSize==0){
			System.err.println("please set the blockSize!");
			System.exit(-1);
		}
		init();
		VCFInfo preInfo=null;
		long position=0;
		VCFBlock block=null;
		headerIndex.setMaxBlockSize(blockSize);
		VCFCodec codec = new VCFCodec();
		try {
			LineReader lineReader = new LineReader(stream, conf);
			Text line=new Text();
			String tempString=null;
			while(lineReader.readLine(line)>0)
			{
				tempString=line.toString();
				//header
				if (tempString.startsWith(METADATA_INDICATOR)) {
                    String[] lineFields = tempString.substring(2).split("=");
                    if (lineFields.length == 2 && isVersionFormatString(lineFields[0]) ) {
                    	headerIndex.setVersion(lineFields[1]);
                    }
                    headerIndex.setHeaderSize(headerIndex.getHeaderSize()+1);
                }else{
                	//block & dic
                	VCFInfo info=new VCFInfo();
                	info.decode(tempString);
                	info.isSorted(preInfo);
                	if(block==null)
                	{
                		block=new VCFBlock();
                		block.setChr(dictionary.get(info.chr));
                		block.setPosition(position);
                		block.setStart(info.pos);
                		block.setBlockSize(block.getBlockSize()+1);
                	}else{
                		if(block.getChr()!=dictionary.get(info.chr)||block.getBlockSize()==blockSize) {
                			block.setEnd(preInfo.end);
                			blocks.get(block.getChr()).add(block);
                			//new block
                			block=new VCFBlock();
                    		block.setChr(dictionary.get(info.chr));
                    		block.setPosition(position);
                    		block.setStart(info.pos);
                    		block.setBlockSize(block.getBlockSize()+1);
                		}else
                			block.setBlockSize(block.getBlockSize()+1);
                	}
                	preInfo=info;
                }
				position +=(line.getLength()+1);
			}
			block.setEnd(preInfo.end);
			blocks.get(block.getChr()).add(block);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}

	private boolean isVersionFormatString(String format)
	{
		format=format.trim();
		return format.equals(VERSION_FORMAT_STRING);
	}
	
	private void init()
	{
		headerIndex=new VCFIndexHeader();
		dictionary=new IndexReferenceDictionary();
		blocks=new HashMap<Integer,ArrayList<VCFBlock>> ();
	}
	

	
	private class VCFInfo{
		String chr;
		int pos;
		int len;
		int end;
		
		public String toString()
		{
			StringBuilder sb=new StringBuilder();
			sb.append(chr);
			sb.append(":");
			sb.append(pos);
			return sb.toString();
		}
		
		void decode(String line)
		{
			String[] vcfFileds=line.split("\t");
			if(vcfFileds.length!=8)
			{
				System.err.println("illegal decode line:"+line);
				System.exit(-1);
			}
			chr=vcfFileds[0];
			pos=Integer.parseInt(vcfFileds[1]);
			len=vcfFileds[3].length();
			end=pos+len-1;
		}
		
		void isSorted(VCFInfo preInfo)
		{
			if(preInfo==null)
			{
				dictionary.add(chr);
				blocks.put(dictionary.get(chr), new ArrayList<VCFBlock>());
				return ;
			}
			if(!chr.equals(preInfo.chr))
			{
				if(dictionary.containsKey(chr)){
					System.err.println("pos"+preInfo.toString()+"--vcf file is not sorted!");
					System.exit(-1);
				}else
					{
					dictionary.add(chr);
					blocks.put(dictionary.get(chr), new ArrayList<VCFBlock>());
					}
			}else if(pos<preInfo.pos)
			{
				System.err.println("pos"+preInfo.toString()+"--vcf file is not sorted!");
				System.exit(-1);
			}
		}
	}
	
	
}
