package org.bgi.flexlab.gaea.data.structure.reads;

import org.bgi.flexlab.gaea.data.structure.bam.ParseSAMBasic;
import org.bgi.flexlab.gaea.data.structure.bam.SAMInformationBasic;
import org.bgi.flexlab.gaea.util.CigarState;

public class ReadInformationForBamToDepth extends SAMInformationBasic{
	
	private int end;
	
	@Override
	protected void parseOtherInfo(String[] alignmentArray) {
		end = ParseSAMBasic.parseCigar(position, cigarState)[0];
	}

	public boolean parseBam2Depth(String value) {

		if (value.isEmpty()) {
			return false;
		}

		String[] alignmentArray = value.split("\t");
		
		flag = Integer.parseInt(alignmentArray[0]);
		
		readSequence = alignmentArray[1];
				
		position = Integer.parseInt(alignmentArray[2]);
		
		if(position < 0 ) {
			return false;
		}
		
		cigarString = alignmentArray[3];
		cigarState = new CigarState();
		cigarState.parseCigar(cigarString);
		end = ParseSAMBasic.parseCigar(position, cigarState)[0];
		
		mappingQual = Short.parseShort(alignmentArray[4]);
		qualityString = alignmentArray[5];
		
		return true;
	}
	
	/**
	 * @return the end
	 */
	public int getEnd() {
		return end;
	}

	/**
	 * @param end the end to set
	 */
	public void setEnd(int end) {
		this.end = end;
	}
}
