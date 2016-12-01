package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.vcf.index.IndexCreator;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;
import org.bgi.flexlab.gaea.util.HdfsFileManager;

import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.index.Block;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;

public class VCFLoader {
	
	private String path;
	
	private Configuration conf = new Configuration();
	
	private VCFCodec codec = new VCFCodec();
	private VCFHeader vcfHeader;
	
	private Index idx;

	public static final Set<String> BLOCK_COMPRESSED_EXTENSIONS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(".gz", ".gzip", ".bgz", ".bgzf")));
	
	public VCFLoader(String vcfFile, Index idx) throws IOException {
		this.idx = idx;
		this.path = vcfFile;
		loadHeader();
	}

	public VCFLoader(String vcfFile) throws IllegalArgumentException, IOException {
		this.path = vcfFile;
		IndexCreator creator = new IndexCreator(vcfFile);
		try {
			this.idx = creator.finalizeIndex();
		} catch (IOException e) {
			e.printStackTrace();
		}
		loadHeader();
	}

	public void loadHeader() throws IOException {
		if (path == null || path.equals(""))
			return;

		InputStream fsInputStream = HdfsFileManager.getInputStream(
				new Path(path), conf);
		if (hasBlockCompressedExtension(path)) {
            // TODO -- warning I don't think this can work, the buffered input stream screws up position
            fsInputStream = new GZIPInputStream(new BufferedInputStream(fsInputStream));
        }
		AsciiLineReader reader = new AsciiLineReader(fsInputStream);
		AsciiLineReaderIterator it = new AsciiLineReaderIterator(reader);
		Object header = codec.readHeader(it);
		it.close();
		vcfHeader = (VCFHeader)(((FeatureCodecHeader)header).getHeaderValue());
	}

	public ArrayList<VariantContext> load(String chr, int start, int end)
			throws IOException {

		if (idx == null)
			return null;
		if (!idx.containsChromosome(chr))
			return null;
		
		List<Block> blocks = idx.getBlocks(chr, start, end);
		if (blocks == null)
			return null;

		ArrayList<VariantContext> context = new ArrayList<VariantContext>();
		long seekPos = blocks.get(0).getStartPosition();
		Text line = new Text();
		String tempString = null;
		FSDataInputStream fsInputStream = HdfsFileManager.getInputStream(
				new Path(path), conf);
		fsInputStream.seek(seekPos);
		LineReader lineReader = new LineReader(fsInputStream, conf);
		while (lineReader.readLine(line) > 0) {
			tempString = line.toString().trim();
			VariantContext var = codec.decode(tempString);
			if (ChromosomeUtils.formatChrName(chr).equals(
					ChromosomeUtils.formatChrName(var.getContig()))) {
				if (var.getStart() < start) {
					continue;
				} else if (var.getStart() >= start && var.getEnd() <= end) {
					context.add(var);
				} else if (var.getStart() > end) {
					break;
				}
			} else {
				break;
			}
		}
		lineReader.close();
		fsInputStream.close();
		if (context.size() == 0)
			return null;

		return context;
	}

	
	public static boolean hasBlockCompressedExtension(String fileName){
		 for (final String extension : BLOCK_COMPRESSED_EXTENSIONS) {
	            if (fileName.toLowerCase().endsWith(extension))
	                return true;
	     }
	     return false;
	}
	
	public VCFHeader getHeader() {
		return vcfHeader;
	}
	
	public static void main(String[] args ) throws IllegalArgumentException, IOException{
		VCFLoader loader = new VCFLoader(args[0]);
		for(VariantContext vc:loader.load("chr1", 1, 10000000))
			System.out.println(vc.getEnd());;
	}
}
