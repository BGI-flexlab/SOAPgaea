package org.bgi.flexlab.gaea.inputformat.vcf.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import htsjdk.variant.variantcontext.VariantContext;

import org.bgi.flexlab.gaea.inputformat.vcf.codec.VCFCodec;
import org.bgi.flexlab.gaea.structure.header.VCFHeader;



public class VCFLoader  {

	private Index<?, ?, VCFBlock> idx;
	private String path;
	private Configuration conf =new Configuration();
	private VCFCodec codec=new VCFCodec();
	private VCFHeader header=null;
	private FSDataInputStream getFSDataInputStream(String path) throws IOException {
		Path p=new Path(path);
		FileSystem fs=p.getFileSystem(conf);
		Path vcfPath = new Path(path);
		return fs.open(vcfPath);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public VCFLoader(String vcfFile,Index idx)
	{
		this.idx=idx;
		this.path=vcfFile;
	}
	
	
	public void loadHeader() throws IOException {
		if(path==null||path.equals("")) {
			return;
		}

		FSDataInputStream fsInputStream = getFSDataInputStream(path);
		LineReader lineReader = new LineReader(fsInputStream, conf);
		
		ArrayList<String> headerLine=new ArrayList<String>();
		Text line=new Text();
		String tempString=null;
		while(lineReader.readLine(line)>0) {
			tempString=line.toString();
			if(tempString.startsWith("#"))
			{
				headerLine.add(tempString.trim());
			} else {
				break;
			}
		}
		lineReader.close();
		fsInputStream.close();
		Object codeHeader=codec.readHeader(headerLine);
		if(codeHeader instanceof VCFHeader) {
			header= (VCFHeader) codeHeader;
		}
	}
	
	public ArrayList<VariantContext> load(String chr,int start,int end) throws IOException {
		
		if(idx==null) { 
			return null;
		}
		if(!idx.containsChromosome(chr)) {
			return null;
		}
		List<VCFBlock> blocks=idx.getBlock(chr, start, end);
		if(blocks==null) {
			return null;
		}
		ArrayList<VariantContext> context=new ArrayList<VariantContext>();
		long seekPos=blocks.get(0).getPosition();
		FSDataInputStream fsInputStream = getFSDataInputStream(path);
		fsInputStream.seek(seekPos);
		LineReader lineReader = new LineReader(fsInputStream, conf);
		
		Text line=new Text();
		String tempString=null;
		while(lineReader.readLine(line)>0) {
			tempString=line.toString().trim();
			VariantContext var= codec.decode(tempString);
        	if(adjustChr(chr).equals(adjustChr(var.getChr()))) {
        		if(var.getStart()<start) {
        			continue;
        		} else if (var.getStart()>=start&&var.getEnd()<=end) {
        			context.add(var);
        		} else if(var.getStart()>end) {
        			break;
        		}
        	} else {
        			break;
        	}
		}
		lineReader.close();
		fsInputStream.close();
		if(context.size()==0) {
			return null;
		}
		return context;
	}
	
	private String adjustChr(String chr) {
		if(chr.contains("chr")) {
			return chr;
		} else {
			return "chr"+chr;
		}
	}
	
	public VCFHeader getHeader() {
		return header;	
	}
}
