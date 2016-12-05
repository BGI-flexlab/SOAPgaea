package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.bgi.flexlab.gaea.data.structure.vcf.index.IndexCreator;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.util.LittleEndianOutputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class VCFLocalLoader {
	
	VCFFileReader reader;
		
	File input, idxFile;
	
	public VCFLocalLoader(String file) throws IOException {
		input = new File(file);
		idxFile = new File(IndexCreator.format(file));
		if(!idxFile.exists()) {
			Index index = IndexFactory.createTabixIndex(input, new VCFCodec(), TabixFormat.VCF, null);
			LittleEndianOutputStream os = new LittleEndianOutputStream(new FileOutputStream(idxFile));
			index.write(os);
			os.close();
		}
		reader = new VCFFileReader(input, idxFile);
	}
	
	public CloseableIterator<VariantContext> iterator() {
		return reader.iterator();
	}
	
	public CloseableIterator<VariantContext> load(String chr, int start, int end) {
		return reader.query(chr, start, end);
	}
	
	public VCFHeader getHeader() {
		return reader.getFileHeader();
	}
	
}
