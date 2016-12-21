package org.bgi.flexlab.gaea.data.structure.memoryshare;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.bgi.flexlab.gaea.data.exception.OutOfBoundException;

public class BioMemoryShare {
	protected int capacity = Byte.SIZE / 4;
	protected String chrName;
	protected int length;
	protected MappedByteBuffer[] byteBuffer = null;
	protected int fcSize = 0;

	protected BioMemoryShare(int capacity) {
		this.capacity = capacity;
	}

	public BioMemoryShare() {
	}

	/**
	 * load bio information;egg : chromosome or dbsnp
	 */
	protected void loadInformation(String path) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(path, "r");
		FileChannel fc = raf.getChannel();
		fcSize = (int) (fc.size() & 0xffffffff);
		int blocks = (int) ((fcSize / Integer.MAX_VALUE) + 1);
		byteBuffer = new MappedByteBuffer[blocks];
		int start = 0;
		long remain = 0;
		int size = 0;
		for (int i = 0; i < blocks; i++) {
			start = Integer.MAX_VALUE * i;
			remain = (long) (fc.size() - start);
			size = (int) ((remain > Integer.MAX_VALUE) ? Integer.MAX_VALUE : remain);
			MappedByteBuffer mapedBB = fc.map(MapMode.READ_ONLY, start, size);
			byteBuffer[i] = mapedBB;
		}
		raf.close();
	}

	public void loadChromosome(String path) {
		try {
			loadInformation(path);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	public String getChromosomeName() {
		return chrName;
	}

	public void setChromosomeName(String chrName) {
		this.chrName = chrName;
	}

	public void setLength(int chrLen) {
		length = chrLen;
	}

	public int getLength() {
		return length;
	}

	public byte[] getBytes(int start, int end) {
		if (start > length)
			throw new OutOfBoundException(length, start);

		byte[] bases;

		int posi = start / capacity;
		int pose;
		if (end >= length) {
			pose = (length - 1) / capacity;
		} else {
			pose = end / capacity;
		}
		bases = new byte[pose - posi + 1];
		byteBuffer[0].position(posi);
		byteBuffer[0].get(bases, 0, pose - posi + 1);
		byteBuffer[0].position(0);

		return bases;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void clean() throws Exception {
		for (MappedByteBuffer buffer : byteBuffer) {
			AccessController.doPrivileged(new PrivilegedAction() {
				@SuppressWarnings("restriction")
				public Object run() {
					try {
						Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
						getCleanerMethod.setAccessible(true);
						sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
						cleaner.clean();
					} catch (Exception e) {
						e.printStackTrace();
					}
					return null;
				}
			});
		}
	}
}
