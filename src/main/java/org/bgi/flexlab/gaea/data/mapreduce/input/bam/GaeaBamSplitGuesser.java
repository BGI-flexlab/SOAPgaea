package org.bgi.flexlab.gaea.data.mapreduce.input.bam;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.FileTruncatedException;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.RuntimeEOFException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaCigar;
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory;
import org.seqdoop.hadoop_bam.util.SeekableArrayStream;

public class GaeaBamSplitGuesser {
	private SeekableStream inFile;
	private SeekableStream in;
	private BlockCompressedInputStream bgzf;
	private final BAMRecordCodec bamCodec;
	private final ByteBuffer buf;
	private final int referenceSequenceCount;
	private static final int SHORTEST_POSSIBLE_BAM_RECORD = 39;

	public GaeaBamSplitGuesser(SeekableStream ss) {
		this.inFile = ss;

		this.buf = ByteBuffer.allocate(8);
		this.buf.order(ByteOrder.LITTLE_ENDIAN);

		this.referenceSequenceCount = new SAMFileReader(ss).getFileHeader()
				.getSequenceDictionary().size();

		this.bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory());
	}

	public long guessNextBAMRecordStart(long beg, long end) throws IOException {
		byte[] arr = new byte[196604];

		this.inFile.seek(beg);

		arr = Arrays.copyOf(
				arr,
				this.inFile.read(arr, 0,
						Math.min((int) (end - beg), arr.length)));

		this.in = new SeekableArrayStream(arr);

		this.bgzf = new BlockCompressedInputStream(this.in);
		this.bgzf.setCheckCrcs(true);

		this.bamCodec.setInputStream(this.bgzf);

		int firstBGZFEnd = Math.min((int) (end - beg), 65535);

		for (int cp = 0;; cp++) {
			PosSize psz = guessNextBGZFPos(cp, firstBGZFEnd);

			if (psz == null) {
				return end;
			}
			int cp0 = cp = psz.pos;
			long cp0Virt = cp0 << 16;
			try {
				this.bgzf.seek(cp0Virt);
			} catch (Throwable e) {
				continue;
			}

			for (int up = 0;; up++) {
				int up0 = up = gaeaGuessNextBAMPos(cp0Virt, up, psz.size);

				if (up < 0) {
					break;
				}

				this.bgzf.seek(cp0Virt | up);
				cp = cp0;
				try {
					for (byte b = 0; ((cp < arr.length ? 1 : 0) & (b < 2 ? 1
							: 0)) != 0;) {
						this.bamCodec.decode();
						int cp2 = (int) (this.bgzf.getFilePointer() >>> 16);
						if (cp2 != cp) {
							assert (cp2 > cp);
							cp = cp2;
							b = (byte) (b + 1);
						}
					}
				} catch (SAMFormatException e) {
					continue;
				} catch (RuntimeEOFException e) {
					continue;
				} catch (FileTruncatedException e) {
					continue;
				} catch (OutOfMemoryError e) {
					continue;
				}
				return beg + cp0 << 16 | up0;
			}
		}
	}

	private PosSize guessNextBGZFPos(int p, int end) {
		try {
			while (true) {
				int n8cnt = 0, n16cnt = 0, n32cnt = 0;
				for (;;) {
					this.in.seek(p);
					this.in.read(this.buf.array(), 0, 4);
					int n = this.buf.getInt(0);

					if (n != 67668767) {
						if (n >>> 8 == 559903) {
							n8cnt++;
							p++;
						} else if (n >>> 16 == 35615) {
							n16cnt++;
							p += 2;
						} else {
							n32cnt++;
							p += 3;
						}
						if (p >= end) {
							System.err.println(n8cnt + "\t" + n16cnt + "\t"
									+ n32cnt);
							return null;
						}
					} else
						break;
				}

				int p0 = p;
				p += 10;
				this.in.seek(p);
				this.in.read(this.buf.array(), 0, 2);
				p += 2;
				int xlen = getUShort(0);
				int subEnd = p + xlen;

				while (p < subEnd) {
					this.in.read(this.buf.array(), 0, 4);

					if (this.buf.getInt(0) != 148290) {
						p += 4 + getUShort(2);
						this.in.seek(p);
					} else {
						this.in.read(this.buf.array(), 0, 2);
						int bsize = getUShort(0);

						p += 6;
						while (p < subEnd) {
							this.in.seek(p);
							this.in.read(this.buf.array(), 0, 4);
							p += 4 + getUShort(2);
						}

						if (p != subEnd) {
							break;
						}

						p += bsize - xlen - 19 + 4;

						this.in.seek(p);

						this.in.read(this.buf.array(), 0, 4);
						int size = this.buf.getInt(0);

						return new PosSize(p0, size);
					}

				}

				p = p0 + 4;
			}
		} catch (IOException e) {
		}
		return null;
	}

	private int gaeaGuessNextBAMPos(long cpVirt, int up, int cSize) {
		up += 4;
		try {
			while (up + SHORTEST_POSSIBLE_BAM_RECORD - 4 < cSize) {
				this.bgzf.seek(cpVirt | up);
				this.bgzf.read(this.buf.array(), 0, 8);

				int id = this.buf.getInt(0);
				int pos = this.buf.getInt(4);

				if ((id < -1) || (id > this.referenceSequenceCount)
						|| (pos < -1)) {
					up++;
				} else {
					this.bgzf.seek(cpVirt | up + 16);
					this.bgzf.read(this.buf.array(), 0, 4);
					int readLength = this.buf.getInt(0);
					if (readLength < 0) {
						up++;
					} else {
						this.bgzf.seek(cpVirt | up + 20);
						this.bgzf.read(this.buf.array(), 0, 8);

						int nid = this.buf.getInt(0);
						int npos = this.buf.getInt(4);

						if ((nid < -1) || (nid > this.referenceSequenceCount)
								|| (npos < -1)) {
							up++;
						} else {
							int nextUP = up + 1;
							up -= 4;

							this.bgzf.seek(cpVirt | up + 12);
							this.bgzf.read(this.buf.array(), 0, 4);

							int nameLength = this.buf.getInt(0) & 0xFF;
							int mappingQual = this.buf.getInt(0) >> 8 & 0xFF;

							if ((mappingQual < 0) || (mappingQual > 255)
									|| (nameLength <= 1)) {
								up = nextUP;
							} else {
								int nullTerminator = up + 36 + nameLength - 1;

								if (nullTerminator >= cSize) {
									up = nextUP;
								} else {
									this.bgzf.seek(cpVirt | up + 16);
									this.bgzf.read(this.buf.array(), 0, 2);
									int cigarLen = this.buf.getInt(0) & 0xFFFF;

									if ((cigarLen < 0)
											|| (cigarLen > readLength)) {
										up = nextUP;
									} else {
										this.bgzf.seek(cpVirt | up + 36
												+ nameLength);
										int size = cigarLen * 4;

										byte[] bs = new byte[size];
										this.bgzf.read(bs, 0, size);
										ByteBuffer bb = ByteBuffer.wrap(bs, 0,
												size);
										bb.order(ByteOrder.LITTLE_ENDIAN);
										try {
											Cigar c = GaeaCigar.decode(bb);
											if ((cigarLen != 0)
													&& (readLength != c
															.getReadLength())) {
												up = nextUP;
											}
										} catch (Exception e) {
											up = nextUP;
										}

										this.bgzf.seek(cpVirt | nullTerminator);
										this.bgzf.read(this.buf.array(), 0, 1);

										if (this.buf.get(0) != 0) {
											up = nextUP;
										} else {
											int zeroMin = 32 + nameLength;

											this.bgzf.seek(cpVirt | up + 16);
											this.bgzf.read(this.buf.array(), 0,
													8);

											zeroMin += (this.buf.getInt(0) & 0xFFFF) * 4;
											zeroMin += this.buf.getInt(4)
													+ (this.buf.getInt(4) + 1)
													/ 2;

											this.bgzf.seek(cpVirt | up);
											this.bgzf.read(this.buf.array(), 0,
													4);

											if (this.buf.getInt(0) < zeroMin) {
												up = nextUP;
											} else
												return up;
										}
									}
								}
							}
						}
					}
				}
			}
		} catch (IOException localIOException) {
		}
		return -1;
	}

	private int getUShort(int idx) {
		return this.buf.getShort(idx) & 0xFFFF;
	}

	private static class PosSize {
		public int pos;
		public int size;

		public PosSize(int p, int s) {
			this.pos = p;
			this.size = s;
		}
	}
}