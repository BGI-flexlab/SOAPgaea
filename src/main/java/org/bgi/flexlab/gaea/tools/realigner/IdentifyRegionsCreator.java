package org.bgi.flexlab.gaea.tools.realigner;

import java.util.ArrayList;
import java.util.Map;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup2.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup2.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup2.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.pileup2.ReadsPool;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.tools.realigner.event.Event;
import org.bgi.flexlab.gaea.tools.realigner.event.EventPair;
import org.bgi.flexlab.gaea.tools.realigner.event.Event.EVENT_TYPE;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;

public class IdentifyRegionsCreator {
	private RealignerOptions option = null;
	private ArrayList<VariantContext> knowIndels = null;
	private ChromosomeInformationShare chr = null;
	private ArrayList<GaeaSamRecord> records = null;
	private GenomeLocationParser parser = null;
	private ArrayList<GenomeLocation> intervals = null;
	private int maxIntervalSize = 500;

	public IdentifyRegionsCreator(RealignerOptions option, ArrayList<GaeaSamRecord> records, SAMFileHeader mHeader,
			ChromosomeInformationShare chr, ArrayList<VariantContext> knowIndels) {
		this.records = records;
		this.knowIndels = knowIndels;
		this.parser = new GenomeLocationParser(mHeader.getSequenceDictionary());
		this.chr = chr;
		this.option = option;
		this.intervals = new ArrayList<GenomeLocation>();
		maxIntervalSize = option.getMaxInterval();
	}

	public Event getEvent(VariantState state, int chrIndex, Pileup pileup, int position) {
		boolean hasIndel = state.isIndel();
		boolean hasInsertion = state.isInsertion();
		boolean hasSNP = state.isSNP();
		int furthestPosition = state.getFurthestPosition();

		byte refBase = chr.getBinaryBase(position - 1);

		boolean lookForMismatchEntropy = option.getMismatchThreshold() > 0 && option.getMismatchThreshold() <= 1 ? true
				: false;

		long totalQuality = 0, mismatchQuality = 0;
		
		pileup.calculateBaseInfo();

		for (PileupReadInfo p : pileup.getPlp()) {
			furthestPosition = Math.max(furthestPosition, p.getAlignmentEnd());

			if (p.isDeletionBase() || p.isNextInsertBase()) {
				hasIndel = true;
				if (p.isNextInsertBase())
					hasInsertion = true;
			} else if (lookForMismatchEntropy) {
				totalQuality += p.getBaseQuality();
				if (p.getBase() != refBase)
					mismatchQuality += p.getBaseQuality();
			}
		}

		if (lookForMismatchEntropy && pileup.getNumberOfElements() >= option.getMinReads()
				&& (double) mismatchQuality / (double) totalQuality >= option.getMismatchThreshold()){
			hasSNP = true;
		}

		if ((!hasIndel && !hasSNP) || (furthestPosition == -1)){
			return null;
		}

		GenomeLocation location = parser.createGenomeLocation(chrIndex, position);
		if (hasInsertion)
			location = parser.createGenomeLocation(location.getContig(), location.getStart(), location.getStart() + 1);

		EVENT_TYPE type = hasSNP ? (hasIndel ? EVENT_TYPE.BOTH : EVENT_TYPE.POINT_EVENT) : EVENT_TYPE.INDEL_EVENT;

		return new Event(location, furthestPosition, type);
	}

	public EventPair setEventPair(EventPair pair, Event event) {
		if (event != null) {
			if (pair.getLeft() == null)
				pair.setLeft(event);
			else if (pair.getRight() == null) {
				if (pair.getLeft().canBeMerge(event)) {
					pair.getLeft().merge(event, option.getSNPWindowSize());
				} else {
					pair.setRight(event);
				}
			} else {
				if (pair.getRight().canBeMerge(event)) {
					pair.getRight().merge(event, option.getSNPWindowSize());
				} else {
					if (pair.getRight().isValidEvent(parser, maxIntervalSize)) {
						pair.getIntervals().add(pair.getRight().getLocation());
						pair.setRight(event);
					}
				}
			}
		}
		return pair;
	}

	public void setIntervals(EventPair pair) {
		if (pair.getLeft() != null && pair.getLeft().isValidEvent(parser, maxIntervalSize))
			pair.getIntervals().add(pair.getLeft().getLocation());
		if (pair.getRight() != null && pair.getRight().isValidEvent(parser, maxIntervalSize))
			pair.getIntervals().add(pair.getRight().getLocation());

		for (GenomeLocation location : pair.getIntervals())
			intervals.add(location);
	}

	public ArrayList<GenomeLocation> getIntervals() {
		return intervals;
	}

	public void regionCreator(int chrIndex,int start, int end) {
		if(records .size() == 0)
			return;
		
		EventPair pair = new EventPair(null, null);
		ReadsPool pool = new ReadsPool(records.iterator(), null);
		
		int regionStart = records.get(0).getAlignmentStart();
		Mpileup mpileup = new Mpileup(pool, regionStart, end-1);

		Map<String, Pileup> pileups = mpileup.getNextPosPileup();
		
		if(pileups == null)
			return;

		if (pileups.size() != 1) {
			throw new RuntimeException("realigner pileup is more than one sample?");
		}

		while (pileups != null) {
			int currPosition = mpileup.getPosition()+1;
			if(currPosition < start){
				pileups = mpileup.getNextPosPileup();
				continue;
			}
			for (String key : pileups.keySet()) {
				VariantState state = new VariantState();
				state.filterVariant(knowIndels, currPosition);
				pair = setEventPair(pair, getEvent(state,chrIndex, pileups.get(key), currPosition));
			}
			pileups = mpileup.getNextPosPileup();
		}

		mpileup.clear();

		setIntervals(pair);
	}
}

