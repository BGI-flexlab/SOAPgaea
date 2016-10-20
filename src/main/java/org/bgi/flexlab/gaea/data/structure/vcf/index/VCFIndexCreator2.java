package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.util.HdfsFileManager;

import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndexCreator;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;

public class VCFIndexCreator2 {
	
	private VCFCodec codec;
	private TabixIndexCreator ctor;
	private Configuration conf;
	
	public VCFIndexCreator2() {
		this.codec = new VCFCodec();
		this.ctor = new TabixIndexCreator(TabixFormat.VCF);
	}
	
	public htsjdk.tribble.index.Index createIndex(String file) throws IOException {
		InputStream is = HdfsFileManager.getInputStream(new Path(file), conf);
		AsciiLineReader lineReader = new AsciiLineReader(is);
		AsciiLineReaderIterator iterator = new AsciiLineReaderIterator(lineReader);
		
		codec.readActualHeader(iterator);
		VariantContext currentContext = null;
		VariantContext lastContext = null;
		while(iterator.hasNext()){	
			long position = iterator.getPosition();
	    	currentContext = codec.decode(iterator.next());
	    	checkSorted(lastContext, currentContext);
	        ctor.addFeature(currentContext, position);
	        lastContext = currentContext;
		}
		iterator.close();
		htsjdk.tribble.index.Index idx = ctor.finalizeIndex(iterator.getPosition());
		return idx;
	}
	
	private void checkSorted(VariantContext lastContext, VariantContext currentContext) {
		if(lastContext != null && lastContext.getStart() > currentContext.getStart() && lastContext.getChr() == currentContext.getChr()
				|| ! isChrSorted(lastContext, currentContext))
			throw new RuntimeException("Input VCF file is not sorted!");
	}
	
	private boolean isChrSorted(VariantContext lastContext, VariantContext currentContext) {
        final Map<String, VariantContext> visitedChromos = new HashMap<String, VariantContext>(40);
		final String curChr = currentContext.getChr();
        final String lastChr = lastContext != null ? lastContext.getChr() : null;
        if(!curChr.equals(lastChr)){
            if(visitedChromos.containsKey(curChr)){
               return false;
            }else{
                visitedChromos.put(curChr, currentContext);
            }
        }
        return true;
	}
}
