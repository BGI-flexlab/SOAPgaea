package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class InformationShare {
	protected MappedByteBuffer[] byteBuffer = null;
	
	/*load information;egg : chromosome dbsnp*/
	public void loadInformation(String path)
			throws IOException {
		RandomAccessFile raf = new RandomAccessFile(path, "r");
		FileChannel fc = raf.getChannel();
		int blocks = (int) ((fc.size() / Integer.MAX_VALUE) + 1);
		byteBuffer = new MappedByteBuffer[blocks];
		int start = 0;
		long remain = 0;
		int size = 0;
		for (int i = 0; i < blocks; i++) {
			start = Integer.MAX_VALUE * i;
			remain = (long) (fc.size() - start);
			size = (int) ((remain > Integer.MAX_VALUE) ? Integer.MAX_VALUE
					: remain);
			MappedByteBuffer mapedBB = fc.map(MapMode.READ_ONLY, start, size);
			byteBuffer[i] = mapedBB;
		}
		raf.close();
	}
}
