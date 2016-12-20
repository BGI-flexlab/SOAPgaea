package org.bgi.flexlab.gaea.data.structure.alignment;

import com.esotericsoftware.kryo.io.Input;

public class AlignmentsBasicCodec extends AlignmentsCodec<AlignmentsBasic>{

	public AlignmentsBasicCodec(Input dataInput) {
		super(dataInput);
	}

	@Override
	public void AlignmentsInit() {
		alignments = new AlignmentsBasic();
	}

	@Override
	protected void writeOtherInfo() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void readOtherInfo() {
		// TODO Auto-generated method stub
		
	}

}
