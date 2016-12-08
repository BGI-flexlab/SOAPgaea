package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.bgi.flexlab.gaea.data.structure.reference.index.vcfIndex;
import org.bgi.flexlab.gaea.data.structure.vcf.index.IndexCreator;
import org.tukaani.xz.SeekableFileInputStream;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.readers.PositionalBufferedStream;
import htsjdk.tribble.util.LittleEndianOutputStream;
import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class VCFLocalLoader {
	
	public static final Set<String> BLOCK_COMPRESSED_EXTENSIONS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(".gz", ".gzip", ".bgz", ".bgzf")));

	VCFFileReader reader;
		
	private String input, idxFile;
		
	private SeekableFileInputStream seekableStream = null; 
	
	private VCFCodec codec;
	
	private FeatureCodecHeader header;
	
	private long pos;
	
	public VCFLocalLoader(String refPath, String dbSNP) throws IOException {
		codec = new VCFCodec();
		input = dbSNP;
		readHeader();
		seekableStream = new SeekableFileInputStream(input);
		idxFile = format(dbSNP);
		if(!(new File(idxFile)).exists()) {
			vcfIndex index = new vcfIndex();
			index.buildIndex(refPath, dbSNP, idxFile);
		}
	}
////	use for test
//	public VCFLocalLoader(String dbSNP) throws IOException {
//		codec = new VCFCodec();
//		input = dbSNP;
//		seekableStream = new SeekableFileInputStream(input);
//		readHeader();
//	}
	
	public Iterator<PositionalVariantContext> iterator() throws IOException {
		return collect();
	}
	
	public Iterator<PositionalVariantContext> iterator(long pos) throws IOException {
		seek(pos);;
		return collect();
	}
	
	private Iterator<PositionalVariantContext> collect() throws IOException {
		AsciiLineReaderIterator iterator = new AsciiLineReaderIterator(
				new AsciiLineReader(seekableStream));
		ArrayList<PositionalVariantContext> variantContexts = new ArrayList<>();
		while(iterator.hasNext()) {
			long pos = iterator.getPosition() + this.pos;
			String line = iterator.next();
			if(line.startsWith(VCFHeader.HEADER_INDICATOR))
				continue;
			VariantContext vc = codec.decode(line);
			variantContexts.add(new PositionalVariantContext(vc, pos));
		}
		return variantContexts.iterator();
	}
	
	public class PositionalVariantContext {
		
		private VariantContext vc;
		
		private long position;
		
		public PositionalVariantContext(VariantContext vc, long position) {
			this.vc = vc;
			this.position = position;
		}
		
		public VariantContext getVariantContext() {
			return vc;
		}
		
		public long getPosition() {
			return position;
		}
	}
	
	public CloseableIterator<VariantContext> query(String chr, int start, int end) {
		return reader.query(chr, start, end);
	}

	private void readHeader() throws IOException {
		InputStream is = null;
        PositionalBufferedStream pbs = null;
        try {
            is = ParsingUtils.openInputStream(input);
            if (hasBlockCompressedExtension(input)) {
                // TODO -- warning I don't think this can work, the buffered input stream screws up position
                is = new GZIPInputStream(new BufferedInputStream(is));
            }
            pbs = new PositionalBufferedStream(is);
            header = codec.readHeader(codec.makeSourceFromStream(pbs));
        } catch (Exception e) {
            throw new TribbleException.MalformedFeatureFile("Unable to parse header with error: " + e.getMessage(), input, e);
        } finally {
            if (pbs != null) pbs.close();
            else if (is != null) is.close();
        }	
	}
	
	public static String format(String inputFile) {
		return inputFile + vcfIndex.INDEX_SUFFIX;
	}
	
	public VCFHeader getHeader() {
		return (VCFHeader)(header.getHeaderValue());
	}
	
	public void seek(long pos) throws IOException {
		this.pos = pos;
		seekableStream.seek(this.pos);
	}
	
	public static boolean hasBlockCompressedExtension(String fileName){
		 for (final String extension : BLOCK_COMPRESSED_EXTENSIONS) {
	            if (fileName.toLowerCase().endsWith(extension))
	                return true;
	     }
	     return false;
	}
	
	public void close() {
		if(seekableStream != null)
			try {
				seekableStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
//	public static void main(String[] args) throws IOException {
//		VCFLocalLoader loader = new VCFLocalLoader("F:\\BGIBigData\\TestData\\VCF\\DNA1425995.vcf");
//		Iterator<PositionalVariantContext> iterator = loader.iterator();
//		while(iterator.hasNext()) {
//			PositionalVariantContext pvc = iterator.next();
//			if(pvc.getVariantContext() == null)
//				System.err.println("pvc is null!");
//			System.out.println(pvc.getPosition() + ":" + pvc.getVariantContext().getChr() + "\t" + pvc.getVariantContext().getStart());
//		}
//		loader.close();
//	}
}
