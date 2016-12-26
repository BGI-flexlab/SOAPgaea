package org.bgi.flexlab.gaea.data.structure.alignment;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.bgi.flexlab.gaea.data.structure.bam.SAMCompressionInformationBasic;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class AlignmentsCodec <T extends SAMCompressionInformationBasic>{
	protected Input dataInput;
	protected Output dataOutput;
	protected T alignments;
	//FieldAccess access = FieldAccess.get(AlignmentsBasic.class);

	public AlignmentsCodec() {

	}

	public AlignmentsCodec(Input dataInput) {
		this.dataInput = dataInput;
		AlignmentsInit();
	}
	
	public AlignmentsCodec(Output dataOutput) {
		this.dataOutput = dataOutput;
		AlignmentsInit();
	}

	public void setInputStream(InputStream input) {
		dataInput = new Input(input);
	}

	public void setOutputStream(OutputStream output) {
		dataOutput = new Output(output);
	}
	
	public void encode(T alignments) {
		writeBasic(alignments);
		writeOtherInfo(alignments);
	}

	public T decode() {
		readBasic();
		readOtherInfo();
		
		return alignments;
	}
	
	private void writeBasic(T alignments) {
		dataOutput.write(alignments.getFlag());
		dataOutput.writeInt(alignments.getChrNameIndex());
		dataOutput.writeInt(alignments.getPosition());
		dataOutput.writeShort(alignments.getMappingQual());
		dataOutput.writeInt(alignments.getCigarsLength());
		int[] cigars = alignments.getCigars();
		for(int i = 0; i < alignments.getCigarsLength(); i++) {
			dataOutput.writeInt(cigars[i]);
		}
		dataOutput.writeInt(alignments.getReadLength());
		dataOutput.write(alignments.getQualities());
		dataOutput.write(alignments.getreadBases());
	}
	
	private void readBasic() {
		alignments.setFlag(dataInput.read());
		alignments.setChrNameIndex(dataInput.read());
		alignments.setPosition(dataInput.read());
		alignments.setMappingQual(dataInput.readShort());
		int cigarLength = dataInput.read();
		int[] cigars = new int[cigarLength];
		for(int i = 0; i < cigarLength; i++) {
			cigars[i] = dataInput.read();
		}
		int readLength = dataInput.read();
		alignments.setQualities(dataInput.readBytes(readLength));
		alignments.setReadBases(dataInput.readBytes((readLength + 1) / 2));
	}
	
	public abstract void AlignmentsInit();

	protected abstract void writeOtherInfo(T alignments);
	
	protected abstract void readOtherInfo();
	
	
}
