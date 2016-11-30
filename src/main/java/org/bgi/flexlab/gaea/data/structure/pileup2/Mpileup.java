package org.bgi.flexlab.gaea.data.structure.pileup2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.bgi.flexlab.gaea.data.mapreduce.writable.ReadInfoWritable;

public class Mpileup {
	/**
	 * reads pool list
	 */
	private Iterator<ReadInfoWritable> readsPool;

	/**
	 * sample -> pileup
	 */
	private Map<String,PileupForMaxQualityBase> plps = new HashMap<String,PileupForMaxQualityBase>();

	/**
	 * end of pileup position
	 */
	private int end;

	/**
	 * position
	 */
	private int position;

	/**
	 * tmp read
	 */
	private ReadInfoWritable tmpRead = null;

	/**
	 * 
	 * @param readsPool
	 * @param position
	 * @param end
	 */
	public Mpileup(Iterator<ReadInfoWritable> readsPool, int position, int end) {
		this.readsPool = readsPool;
		this.position = position;
		this.end = end;
	}

	private void addReads2Pileup(ReadInfo read, int pos) {
		PileupForMaxQualityBase plp = plps.get(read.getSample());
		if (plp == null) {
			//plp = new NiftyPileup();
			plp = new PileupForMaxQualityBase();
			plp.setPosition(pos);
			plps.put(read.getSample(), plp);
		}
		plp.addReads(read);
	}

	public boolean allEmpty() {
		boolean allEmpty = true;
		for (String sample : plps.keySet()) {
			PileupForMaxQualityBase plp = plps.get(sample);
			if (!plp.isEmpty()) {
				allEmpty = false;
				break;
			}
		}

		return allEmpty;
	}
	
	private int forwardPosition(int minPosition, int size) {
		int minimumPosition = Integer.MAX_VALUE;
		// forward position
		@SuppressWarnings("rawtypes")
		java.util.Iterator it = plps.entrySet().iterator();
		while(it.hasNext()){
			@SuppressWarnings("rawtypes")
			java.util.Map.Entry entry = (java.util.Map.Entry) it.next();
			PileupForMaxQualityBase plp = (PileupForMaxQualityBase)entry.getValue();
			if (plp.getPosition() == minPosition) {
				plp.forwardPosition(size);
			}
			
			if(plp.getPosition() < minimumPosition && !plp.isEmpty())
				minimumPosition = plp.getPosition();
			else if(plp.isEmpty()){
				it.remove();
			}
		}
		
		return minimumPosition;
	}

	private int addReads(int minPosition) {
		int currentPosition = minPosition;

		if (readsPool.hasNext() && tmpRead == null) {
			tmpRead = readsPool.next();
		}

		while (tmpRead != null) {
			ReadInfo read = new ReadInfo();
			read.parseAFC(tmpRead);
			if (currentPosition == Integer.MAX_VALUE
					|| currentPosition == read.getPosition()) {
				if (currentPosition == Integer.MAX_VALUE)
					currentPosition = read.getPosition();
				addReads2Pileup(read, currentPosition);
				if (readsPool.hasNext())
					tmpRead = readsPool.next();
				else
					tmpRead = null;
			} else {
				break;
			}
		}
		return currentPosition;
	}
	
	private void syn(int minPosition,Map<String, PileupForMaxQualityBase> posPlps) {
		@SuppressWarnings("rawtypes")
		Iterator it = plps.entrySet().iterator();
		while(it.hasNext()){
			@SuppressWarnings("rawtypes")
			java.util.Map.Entry entry = (java.util.Map.Entry) it.next();
			PileupForMaxQualityBase plp = (PileupForMaxQualityBase)entry.getValue();
			if (plp.getPosition() == minPosition) {
				String sample = (String)entry.getKey();
				posPlps.put(sample, plp);
			}
		}
	}
	

	public Map<String,PileupForMaxQualityBase> getNextPosPileup(){
		if (position > end)
			return null;
		
		int minPosition = forwardPosition(position, 1);
		
		position = addReads(minPosition);
		
		if (minPosition != Integer.MAX_VALUE && position != minPosition)
			throw new RuntimeException("error in" + position + "\t"
					+ minPosition);
		if (position > end || allEmpty())
			return null;
		
		Map<String,PileupForMaxQualityBase> posPlps = new HashMap<String,PileupForMaxQualityBase>();
		syn(position,posPlps);

		return posPlps;
	}

	public int getMinPositionInPlp() {
		int minPosition = Integer.MAX_VALUE;

		for (String sample : plps.keySet()) {
			PileupForMaxQualityBase plp = plps.get(sample);
			if (minPosition > plp.getPosition() && !plp.isEmpty()) {
				minPosition = plp.getPosition();
			}
		}

		return minPosition;
	}

	public int getPosition() {
		return position;
	}
}

