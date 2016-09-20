package org.bgi.flexlab.gaea.tools.annotator.inputformat;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

import java.io.File;
import java.util.Iterator;
import java.util.List;

public class VCFReader extends VCFFileReader {
	
	/** Constructs a VCFReader that requires the index to be present. */
	public VCFReader(final File file) {
		super(file, true);
	}

	/** Constructs a VCFReader with a specified index. */
	public VCFReader(File file, boolean requireIndex) {
		super(file, requireIndex);
	}
	
	public List<VariantContext> getVariantList(){
		
//		CloseableIterator<VariantContext> vcIt= super.iterator();
//		for (; vcIt.hasNext();) {
//			VariantContext type = (VariantContext) vcIt.next();
//			
//		}
		
		List<VariantContext> vcList = this.iterator().toList();
//		for (Iterator iterator = vcList.iterator(); iterator.hasNext();) {
//			VariantContext variantContext = (VariantContext) iterator.next();
//			
//		}
		
//		vcIt.close();
		return vcList;
		
	}
	
	
	public List<VariantContext> getBedVariantList(){
		return null;
//		super.query(arg0, arg1, arg2);
//		
//		return vcList;
		
	}

}
