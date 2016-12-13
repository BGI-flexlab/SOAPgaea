package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.tukaani.xz.SeekableFileInputStream;

import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.util.ParsingUtils;

public class VCFLocalLoader extends AbstractVCFLoader{

	private SeekableFileInputStream seekableStream;

	private long pos;

	public VCFLocalLoader(String dbSNP) throws IOException {
		super(dbSNP);
	}

	
	@Override
	public void seek(long pos){
		this.pos = pos;
		try {
			seekableStream.seek(this.pos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e.toString());
		}
		iterator = new AsciiLineReaderIterator(new AsciiLineReader(seekableStream));
	}


	@Override
	public void getSeekableStream() {
		// TODO Auto-generated method stub
		try {
			seekableStream = new SeekableFileInputStream(input);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e.toString());
		}
	}


	@Override
	public InputStream getInputStream(String input) throws IOException {
		// TODO Auto-generated method stub
		return ParsingUtils.openInputStream(input);
	}

}
