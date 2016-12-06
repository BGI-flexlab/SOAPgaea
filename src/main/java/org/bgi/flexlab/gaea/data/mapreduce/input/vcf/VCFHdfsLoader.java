package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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

public class VCFHdfsLoader {
	
	private String path;
	
	private Configuration conf = new Configuration();
	
	private VCFCodec codec = new VCFCodec();
	private Object header;
	
	private InputStream is;
	
	private AsciiLineReaderIterator iterator;
	
	private Index idx;

	public static final Set<String> BLOCK_COMPRESSED_EXTENSIONS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(".gz", ".gzip", ".bgz", ".bgzf")));
	
	public VCFHdfsLoader(String vcfFile, Index idx) throws IOException {
		this.idx = idx;
		this.path = vcfFile;
		loadHeader();
	}

	public VCFHdfsLoader(String vcfFile) throws IllegalArgumentException, IOException {
		this.path = vcfFile;
		IndexCreator creator = new IndexCreator(vcfFile);
		try {
			this.idx = creator.finalizeIndex();
		} catch (IOException e) {
			e.printStackTrace();
		}
		loadHeader();
	}

	public void initInputStream() throws IOException {
		if (path == null || path.equals(""))
			return;

		is = new BufferedInputStream(HdfsFileManager.getInputStream(new Path(path), conf), 512000);
		if (hasBlockCompressedExtension(path)) {
            // TODO -- warning I don't think this can work, the buffered input stream screws up position
            is = new GZIPInputStream(new BufferedInputStream(is, 512000));
        }
	}
	
	public void loadHeader() throws IOException {
		initInputStream();
		AsciiLineReader reader = new AsciiLineReader(is);
		iterator = new AsciiLineReaderIterator(reader);
		header = codec.readHeader(iterator);
	}

	public Iterator<VariantContext> iterator() throws IOException {
		ArrayList<VariantContext> result = new ArrayList<>();
		while(iterator.hasNext()) {
			result.add(codec.decode(iterator.next()));;
		}
		iterator.close();
		return result.iterator();
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
		return (VCFHeader)(((FeatureCodecHeader)header).getHeaderValue());
	}
	
	public static void main(String[] args ) throws IllegalArgumentException, IOException{
		VCFHdfsLoader loader = new VCFHdfsLoader(args[0]);
		Iterator<VariantContext> iterator = loader.iterator();
		while(iterator.hasNext()) {
			VariantContext vContext = iterator.next();
			System.out.println(vContext.getChr() + "\t" + vContext.getStart());
		}
	}
}
