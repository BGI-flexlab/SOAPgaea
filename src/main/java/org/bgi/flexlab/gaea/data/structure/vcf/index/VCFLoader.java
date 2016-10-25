package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.vcf.GaeaVCFCodec;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;
import org.bgi.flexlab.gaea.util.HdfsFileManager;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

public class VCFLoader {
	private Index<?, ?, VCFBlock> idx;
	private String path;
	private Configuration conf = new Configuration();
	private GaeaVCFCodec codec = new GaeaVCFCodec();
	private VCFHeader header = null;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public VCFLoader(String vcfFile, Index idx) {
		this.idx = idx;
		this.path = vcfFile;
	}

	@SuppressWarnings({"unchecked" })
	public VCFLoader(String vcfFile) {
		this.path = vcfFile;
		VCFIndexCreator creator = new VCFIndexCreator();
		creator.initialize(path, creator.defaultBinSize());
		try {
			this.idx = creator.finalizeIndex();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadHeader() throws IOException {
		if (path == null || path.equals(""))
			return;

		ArrayList<String> headerLine = new ArrayList<String>();
		Text line = new Text();
		String tempString = null;
		FSDataInputStream fsInputStream = HdfsFileManager.getInputStream(
				new Path(path), conf);
		LineReader lineReader = new LineReader(fsInputStream, conf);
		while (lineReader.readLine(line) > 0) {
			tempString = line.toString();
			if (tempString.startsWith("#")) {
				headerLine.add(tempString.trim());
			} else {
				break;
			}
		}
		lineReader.close();
		fsInputStream.close();
		Object codeHeader = codec.readHeader(headerLine);
		if (codeHeader instanceof VCFHeader) {
			header = (VCFHeader) codeHeader;
		}
	}

	public ArrayList<VariantContext> load(String chr, int start, int end)
			throws IOException {

		if (idx == null)
			return null;
		if (!idx.containsChromosome(chr))
			return null;
		List<VCFBlock> blocks = idx.getBlock(chr, start, end);
		if (blocks == null)
			return null;

		ArrayList<VariantContext> context = new ArrayList<VariantContext>();
		long seekPos = blocks.get(0).getPosition();
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
					ChromosomeUtils.formatChrName(var.getChr()))) {
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

	public VCFHeader getHeader() {
		return header;

	}
}
