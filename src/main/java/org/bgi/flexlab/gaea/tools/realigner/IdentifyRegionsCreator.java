package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupContext;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;
import org.bgi.flexlab.gaea.data.structure.pileup.manager.PileupState;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.tools.realigner.event.Event;
import org.bgi.flexlab.gaea.tools.realigner.event.Event.EVENT_TYPE;
import org.bgi.flexlab.gaea.tools.realigner.event.EventPair;

public class IdentifyRegionsCreator {
	private RealignerOptions option = null;
	private ArrayList<VariantContext> knowIndels = null;
	private ChromosomeInformationShare chr = null;
	private ArrayList<GaeaSamRecord> records = null;
	private GenomeLocationParser parser = null;
	private ArrayList<GenomeLocation> intervals = null;
	private int maxIntervalSize = 500;

	public IdentifyRegionsCreator(RealignerOptions option,
			ArrayList<GaeaSamRecord> records, SAMFileHeader mHeader,
			ChromosomeInformationShare chr, ArrayList<VariantContext> knowIndels) {
		this.records = records;
		this.knowIndels = knowIndels;
		this.parser = new GenomeLocationParser(mHeader.getSequenceDictionary());
		this.chr = chr;
		this.option = option;
		this.intervals = new ArrayList<GenomeLocation>();
		maxIntervalSize = option.getMaxInterval();
	}

	public Event getEvent(VariantState state, PileupContext context) {
		boolean hasIndel = state.isIndel();
		boolean hasInsertion = state.isInsertion();
		boolean hasSNP = state.isSNP();
		int furthestPosition = state.getFurthestPosition();

		GenomeLocation location = context.getLocation();
		Pileup pileup = context.getBasePileup();
		byte refBase = chr.getBinaryBase(location.getStart() - 1);

		boolean lookForMismatchEntropy = option.getMismatchThreshold() > 0
				&& option.getMismatchThreshold() <= 1 ? true : false;

		long totalQuality = 0, mismatchQuality = 0;

		for (PileupElement p : pileup) {
			furthestPosition = Math.max(furthestPosition, p.getRead()
					.getAlignmentEnd());

			if (p.isDeletion() || p.isBeforeInsertion()) {
				hasIndel = true;
				if (p.isBeforeInsertion())
					hasInsertion = true;
			} else if (lookForMismatchEntropy) {
				totalQuality += p.getQuality();
				if (p.getBase() != refBase)
					mismatchQuality += p.getQuality();
			}
		}

		if (lookForMismatchEntropy
				&& pileup.getNumberOfElements() >= option.getMinReads()
				&& (double) mismatchQuality / (double) totalQuality >= option
						.getMismatchThreshold())
			hasSNP = true;

		if ((!hasIndel && !hasSNP) || (furthestPosition == -1))
			return null;

		if (hasInsertion)
			location = parser.createGenomeLocation(location.getContig(),
					location.getStart(), location.getStart() + 1);

		EVENT_TYPE type = hasSNP ? (hasIndel ? EVENT_TYPE.BOTH
				: EVENT_TYPE.POINT_EVENT) : EVENT_TYPE.INDEL_EVENT;

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
		if (pair.getLeft() != null
				&& pair.getLeft().isValidEvent(parser, maxIntervalSize))
			pair.getIntervals().add(pair.getLeft().getLocation());
		if (pair.getRight() != null
				&& pair.getRight().isValidEvent(parser, maxIntervalSize))
			pair.getIntervals().add(pair.getRight().getLocation());

		for (GenomeLocation location : pair.getIntervals())
			intervals.add(location);
	}
	
	public ArrayList<GenomeLocation> getIntervals(){
		return intervals;
	}

	public void regionCreator() {
		EventPair pair = new EventPair(null, null);
		PileupState pState = new PileupState(records, parser);
		PileupContext context = null;
		while (pState.hasNext()) {
			context = pState.next();
			VariantState state = new VariantState();
			state.filterVariant(knowIndels, context.getPosition());
			pair = setEventPair(pair, getEvent(state, context));
		}
		
		pState = null;
		context = null;
		
		setIntervals(pair);
	}
}
