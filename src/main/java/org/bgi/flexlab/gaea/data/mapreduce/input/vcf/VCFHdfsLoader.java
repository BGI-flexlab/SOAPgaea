package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.structure.vcf.AbstractVCFLoader;
import org.bgi.flexlab.gaea.util.HdfsFileManager;
import org.seqdoop.hadoop_bam.util.WrapSeekable;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;

public class VCFHdfsLoader extends AbstractVCFLoader{

	private Configuration conf = new Configuration();

	private WrapSeekable seekableStream;
	
	public VCFHdfsLoader(String dbSNP) throws IllegalArgumentException, IOException {
		super(dbSNP);
	}
	
	@Override
	public void seek(long pos) {
		this.pos = pos;
		try {
			seekableStream.seek(pos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
		iterator = new AsciiLineReaderIterator(new AsciiLineReader(seekableStream));
	}

	@Override
	public void getSeekableStream() {
		// TODO Auto-generated method stub
		try {
			seekableStream = WrapSeekable.openPath(conf, new Path(super.input));
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

	@Override
	public InputStream getInputStream(String input) {
		// TODO Auto-generated method stub
		return HdfsFileManager.getInputStream(new Path(super.input), conf);
	}
}
