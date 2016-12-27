package org.bgi.flexlab.gaea.data.structure.alignment;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AlignmentsBasicCodec extends AlignmentsCodec<AlignmentsBasic>{

	public AlignmentsBasicCodec() {

	}
	public AlignmentsBasicCodec(Input dataInput) {
		super(dataInput);
	}

	public AlignmentsBasicCodec(Output dataOutput) {
		super(dataOutput);
	}

	@Override
	public void AlignmentsInit() {
		alignments = new AlignmentsBasic();
	}

	@Override
	protected void writeOtherInfo(AlignmentsBasic alignments) {
		// TODO Auto-generated method stub
		dataOutput.write(alignments.getSampleIndex());
	}

	@Override
	protected void readOtherInfo() {
		// TODO Auto-generated method stub
		alignments.setSampleIndex(dataInput.read());
	}

}
