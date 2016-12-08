package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.util.HdfsFileManager;

import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.util.LittleEndianInputStream;
import htsjdk.tribble.util.LittleEndianOutputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;

public class WindowBasedIndexCreator {
	
	private int winSize;
	
	private String file;
		
	private BufferedInputStream is;
		
	private Configuration conf;
	
	private FileSystem fs;
	
	private WindowBasedIndex index;
	
	public WindowBasedIndexCreator(String file, int winSize) throws Exception {
		conf = new Configuration();
		this.file = file;
		fs = new Path(file).getFileSystem(conf);
		this.winSize = winSize;
	}
	
	public WindowBasedIndex finalizeIndex() throws IllegalArgumentException, IOException {
		Path idxfile = new Path(format(winSize, file));
		FSDataInputStream is = null;
		try {
			if(fs.exists(idxfile)) {
				is = fs.open(idxfile);
				index = new WindowBasedIndex(is);
			} else {
				is = fs.open(new Path(file));
				index = new WindowBasedIndex(winSize);
				createIndex();
				OutputStream os = HdfsFileManager.getOutputStream(idxfile, conf);
				index.write(os);
				os.close();
			}
		} finally {
			fs.close();
		} 
		
		return index; 
	}
	
	public void createIndex() {
		VCFCodec codec = new VCFCodec();
		VariantContext lastContext = null;
        VariantContext currentContext;
        int lastWin = -1;
        int currentWin = -1;
        final Map<String, VariantContext> visitedChromos = new HashMap<String, VariantContext>();
        AsciiLineReader lineReader = new AsciiLineReader(is);
        AsciiLineReaderIterator iterator = new AsciiLineReaderIterator(lineReader);
        codec.readActualHeader(iterator);
        IndexDatum datum = new IndexDatum();
        while(iterator.hasNext()) {
        	long pos = iterator.getPosition();
        	currentContext = codec.decode(iterator.next());
        	checkSorted(lastContext, currentContext);
            //should only visit chromosomes once
            final String curChr = currentContext.getChr();
            final String lastChr = lastContext != null ? lastContext.getChr() : null;
            if(!curChr.equals(lastChr)){
                if(visitedChromos.containsKey(curChr)){
                	throw new RuntimeException("Input file must have contiguous chromosomes.");
                }else{
                    visitedChromos.put(curChr, currentContext);
                }
            }
            
            currentWin = currentContext.getStart() / winSize;
            if(currentWin != lastWin) {
            	datum.setChr(curChr);
            	datum.setWinID(currentWin);
            	datum.setPos(pos);
            	if(datum.getEnd() > 0) {
            		datum.setId2interval();
            	}
            } 
            if(lastChr != null && !curChr.equals(lastChr)) {
            	index.combine(datum);
            	datum = new IndexDatum();
            }
            	
            lastWin = currentWin;
            lastContext = currentContext;
        }
	}
	

	private static void checkSorted(final VariantContext lastContext, final VariantContext currentContext){
	        // if the last currentFeature is after the current currentFeature, exception out
        if (lastContext != null && currentContext.getStart() < lastContext.getStart() && lastContext.getChr().equals(currentContext.getChr()))
            throw new RuntimeException("Input file is not sorted by start position.");
	}
	
	public static String format(int winSize,String inputFile) {
		return inputFile + ".winSize." + winSize +".gaeaidx";
	}
}
