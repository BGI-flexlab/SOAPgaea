package org.bgi.flexlab.gaea.data.structure.pileup2;

import java.util.ArrayList;

public class NiftyPileup extends Pileup{
	@Override
	public ArrayList<PileupReadInfo> getPileup() {
		if(plp.size() == 0)
			return plp;
		
		ArrayList<PileupReadInfo> finalPlp = new ArrayList<PileupReadInfo>();
		PileupReadInfo posReadMax = plp.get(0);
		if(plp.size() > 1){
			byte maxBaseQuality = posReadMax.getByteBaseQuality();
			for(int i = 1; i < plp.size(); i++){
				PileupReadInfo posRead = plp.get(i);
				if(maxBaseQuality < posRead.getByteBaseQuality()){
					posReadMax = posRead;
					maxBaseQuality = posRead.getByteBaseQuality();
				}
			}
		}
		
		finalPlp.add(posReadMax);   //plp.size() == 1鏃朵篃瑕佹坊鍔�
		return finalPlp;
	}
}
