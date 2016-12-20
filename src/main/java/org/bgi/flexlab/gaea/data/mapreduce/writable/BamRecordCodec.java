package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.InputStream;

import htsjdk.samtools.BAMRecord;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.DefaultSAMRecordFactory;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordFactory;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.RuntimeEOFException;

public class BamRecordCodec extends BAMRecordCodec{

	static final int FIXED_BLOCK_SIZE = 8 * 4;
	
	private SAMFileHeader header = null;
	private final BinaryCodec binaryCodec = new BinaryCodec();
    private final SAMRecordFactory samRecordFactory;
	
	public BamRecordCodec(final SAMFileHeader header) {
		this(header,new DefaultSAMRecordFactory());
	}
	
	public BamRecordCodec(final SAMFileHeader header, final SAMRecordFactory factory) {
        super(header,factory);
        this.header = header;
        this.samRecordFactory = factory;
    }
	
	public void setInputStream(final InputStream is) {
	    this.binaryCodec.setInputStream(is);
	}
	
	public SAMRecord decode() {
        int recordLength = 0;
        try {
            recordLength = this.binaryCodec.readInt();
        }
        catch (RuntimeEOFException e) {
            return null;
        }

        if (recordLength < FIXED_BLOCK_SIZE) {
            throw new SAMFormatException("Invalid record length: " + recordLength);
        }
        
        final int referenceID = this.binaryCodec.readInt();
        final int coordinate = this.binaryCodec.readInt() + 1;
        final short readNameLength = this.binaryCodec.readUByte();
        final short mappingQuality = this.binaryCodec.readUByte();
        final int bin = this.binaryCodec.readUShort();
        final int cigarLen = this.binaryCodec.readUShort();
        final int flags = this.binaryCodec.readUShort();
        final int readLen = this.binaryCodec.readInt();
        final int mateReferenceID = this.binaryCodec.readInt();
        final int mateCoordinate = this.binaryCodec.readInt() + 1;
        final int insertSize = this.binaryCodec.readInt();
        final byte[] restOfRecord = new byte[recordLength - FIXED_BLOCK_SIZE];
        this.binaryCodec.readBytes(restOfRecord);
        final BAMRecord ret = this.samRecordFactory.createBAMRecord(
                header, referenceID, coordinate, readNameLength, mappingQuality,
                bin, cigarLen, flags, readLen, mateReferenceID, mateCoordinate, insertSize, restOfRecord);

        if (null != header) {
            // don't reset a null header as this will clobber the reference and mate reference indices
            ret.setHeader(header);
        }
        return ret;
    }
}
