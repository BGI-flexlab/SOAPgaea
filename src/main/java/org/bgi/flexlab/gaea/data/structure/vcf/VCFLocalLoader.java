package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.GZIPInputStream;

//import org.bgi.flexlab.gaea.data.structure.reference.index.VcfIndex;
import org.tukaani.xz.SeekableFileInputStream;

import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.readers.PositionalBufferedStream;
import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;

public class VCFLocalLoader {
	
	public static final Set<String> BLOCK_COMPRESSED_EXTENSIONS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(".gz", ".gzip", ".bgz", ".bgzf")));
		
	private String input;
		
	private SeekableFileInputStream seekableStream = null; 
	
	private AsciiLineReaderIterator iterator;
	
	private VCFCodec codec;
	
	private FeatureCodecHeader header;
	
	private long pos;
	
	public VCFLocalLoader(String dbSNP) throws IOException {
		codec = new VCFCodec();
		input = dbSNP;
		readHeader();
		seekableStream = new SeekableFileInputStream(input);
		seek(0);
	}
	
	public boolean hasNext() {
		return iterator.hasNext();
	}
	
	public PositionalVariantContext next() {
		long pos = iterator.getPosition() + this.pos;
		String line = iterator.next();
		while(line.startsWith(VCFHeader.HEADER_INDICATOR)) {
			pos = iterator.getPosition() + this.pos;
			line = iterator.next();
		}
		VariantContext vc = codec.decode(line);
		PositionalVariantContext pvc = new PositionalVariantContext(vc, pos);
		return pvc;
	}
	
	@Deprecated
	public Iterator<PositionalVariantContext> iterator() throws IOException {
		return collect();
	}
	
	@Deprecated
	public Iterator<PositionalVariantContext> iterator(long pos) throws IOException {
		seek(pos);;
		return collect();
	}
	
	private Iterator<PositionalVariantContext> collect() throws IOException {
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
	
//	FIXME:function need to be fulfilled
	public void query(String chr, int start, int end) {

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
	
//	public static String format(String inputFile) {
//		return inputFile + VcfIndex.INDEX_SUFFIX;
//	}
	
	public VCFHeader getHeader() {
		return (VCFHeader)(header.getHeaderValue());
	}
	
	public void seek(long pos) throws IOException {
		this.pos = pos;
		seekableStream.seek(this.pos);
		iterator = new AsciiLineReaderIterator(new AsciiLineReader(seekableStream));
	}
	
	public static boolean hasBlockCompressedExtension(String fileName){
		 for (final String extension : BLOCK_COMPRESSED_EXTENSIONS) {
	            if (fileName.toLowerCase().endsWith(extension))
	                return true;
	     }
	     return false;
	}
	
	public void close() {
		if(iterator != null)
			try {
				iterator.close();
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
	}
	
}
