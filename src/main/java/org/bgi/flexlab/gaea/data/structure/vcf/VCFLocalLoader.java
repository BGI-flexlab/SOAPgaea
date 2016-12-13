package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.tukaani.xz.SeekableFileInputStream;

import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.util.ParsingUtils;

public class VCFLocalLoader  extends AbstractVCFLoader {
					
	private SeekableFileInputStream seekableStream; 
		
	public VCFLocalLoader(String dbSNP) throws IOException {
		super(dbSNP);
	}
	
	@Override
	public void seek(long pos) {
		this.pos = pos;
		try {
			seekableStream.seek(this.pos);
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
			seekableStream = new SeekableFileInputStream(input);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

	@Override
	public InputStream getInputStream(String input) {
		// TODO Auto-generated method stub
		try {
			return ParsingUtils.openInputStream(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String[] args) throws IOException {
		VCFLocalLoader loader = new VCFLocalLoader("F:\\BGIBigData\\TestData\\VCF\\DNA1425995.vcf");
		loader.seek(6287);
		while(loader.hasNext()) {
			PositionalVariantContext pvc = loader.next();
			System.out.println(pvc.getPosition() + ":" +
								pvc.getVariantContext().getChr() + "\t" +
								pvc.getVariantContext().getStart());
		}
	}
}
