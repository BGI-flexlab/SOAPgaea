package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTag;
import htsjdk.samtools.util.SequenceUtil;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaAlignedSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecordBin;
import org.bgi.flexlab.gaea.data.structure.bam.filter.RealignerFilter;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.location.RealignerIntervalFilter;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.tools.realigner.alternateconsensus.AlternateConsensus;
import org.bgi.flexlab.gaea.tools.realigner.alternateconsensus.AlternateConsensusEngine;
import org.bgi.flexlab.gaea.util.AlignmentUtil;
import org.bgi.flexlab.gaea.util.Pair;
import org.bgi.flexlab.gaea.util.Window;
import org.bgi.flexlab.gaea.variant.filter.VariantRegionFilter;

public class IndelRealigner {
	private static int MAX_READS = 20000;
	private ArrayList<GenomeLocation> intervals = null;
	private ArrayList<VariantContext> variants = null;
	private ChromosomeInformationShare chrInfo = null;
	private GenomeLocationParser parser = null;
	private VariantRegionFilter filter = null;
	private Window win = null;
	private RealignerOptions option = null;
	private AlternateConsensusEngine consensusEngine = null;
	private GenomeLocation currentLocation = null;
	private Iterator<GenomeLocation> iterator = null;
	private GenomeLocation currentInterval = null;
	private GaeaSamRecordBin needRealignementReads = null;
	private ArrayList<GaeaSamRecord> notNeedRealignementReads = null;
	private TreeSet<VariantContext> knowIndelsSet = null;
	private int effectiveNotCleanReadCount = 0;

	public IndelRealigner(SAMFileHeader mHeader,
			ArrayList<VariantContext> variants, Window win,
			ChromosomeInformationShare chrInfo, RealignerOptions option) {
		this.parser = new GenomeLocationParser(mHeader.getSequenceDictionary());
		this.variants = variants;
		this.chrInfo = chrInfo;
		this.option = option;
		initialization();
	}

	@SuppressWarnings("unchecked")
	private void initialization() {
		filter = new VariantRegionFilter();
		consensusEngine = new AlternateConsensusEngine(
				option.getConsensusModel(), option.getMismatchThreshold(),
				option.getLODThreshold());
		needRealignementReads = new GaeaSamRecordBin(parser);
		notNeedRealignementReads = new ArrayList<GaeaSamRecord>();
		knowIndelsSet = new TreeSet<VariantContext>(new KnowIndelComparator());
	}

	public void setIntervals(ArrayList<GenomeLocation> intervals) {
		this.intervals = intervals;
		iterator = this.intervals.iterator();
		currentInterval = iterator.hasNext() ? iterator.next() : null;
	}

	private ArrayList<VariantContext> filterKnowIndels(GaeaSamRecord read) {
		GenomeLocation location = parser.createGenomeLocation(read);
		return filter.listFilter(variants, location.getStart(),
				location.getStop());
	}

	private void updateWindowByInterval() {
		RealignerIntervalFilter filter = new RealignerIntervalFilter();
		ArrayList<GenomeLocation> filtered = filter.filterList(intervals, win);

		if (filtered == null || filtered.isEmpty()) {
			return;
		}

		GenomeLocation start = filtered.get(0);
		if (start.getStart() < win.getStart()
				&& start.getStop() >= win.getStart()) {
			win.setStart(start.getStop() + 1);
		}

		GenomeLocation end = filtered.get(filtered.size() - 1);
		if (end.getStart() <= win.getStop() && end.getStop() > win.getStop()) {
			win.setStop(end.getStop());
		}
		filtered.clear();
	}

	private void realignerCore(AlternateConsensus bestConsensus,
			ArrayList<GaeaAlignedSamRecord> reads, byte[] ref,
			GenomeLocation location, long totalRawMismatchQuality,
			int leftMostIndex) {
		double improvement = (bestConsensus == null ? -1
				: ((double) (totalRawMismatchQuality - bestConsensus
						.getMismatch())) / 10.0);

		int threshold = 0;
		if (improvement < threshold)
			return;

		int posOnRef = bestConsensus.getPositionOnReference();
		Cigar newCigar = AlignmentUtil.leftAlignIndel(bestConsensus.getCigar(),
				ref, bestConsensus.getSequence(), posOnRef, posOnRef);
		bestConsensus.setCigar(newCigar);

		GaeaAlignedSamRecord alignRead = null;
		for (Pair<Integer, Integer> pair : bestConsensus.getReadIndexes()) {
			alignRead = reads.get(pair.first);
			if (!consensusEngine.updateRead(bestConsensus.getCigar(), posOnRef,
					pair.second, alignRead, leftMostIndex))
				return;
		}

		if (consensusEngine.needRealignment(reads, ref, leftMostIndex)) {
			for (Pair<Integer, Integer> indexPair : bestConsensus
					.getReadIndexes()) {
				alignRead = reads.get(indexPair.first);
				if (alignRead.statusFinalize()) {
					GaeaSamRecord read = alignRead.getRead();

					if (read.getMappingQuality() != 255)
						read.setMappingQuality(Math.min(alignRead.getRead()
								.getMappingQuality() + 10, 254));

					int neededBasesToLeft = leftMostIndex
							- read.getAlignmentStart();
					int neededBasesToRight = read.getAlignmentEnd()
							- leftMostIndex - ref.length + 1;
					int neededBases = Math.max(neededBasesToLeft,
							neededBasesToRight);
					if (neededBases > 0) {
						int padLeft = Math.max(leftMostIndex - neededBases, 1);
						int padRight = Math.min(leftMostIndex + ref.length
								+ neededBases,
								parser.getContigInfo(location.getContig())
										.getSequenceLength());
						ref = chrInfo.getBaseSequence(padLeft, padRight)
								.getBytes();
						leftMostIndex = padLeft;
					}

					try {
						if (read.getAttribute(SAMTag.NM.name()) != null)
							read.setAttribute(SAMTag.NM.name(), SequenceUtil
									.calculateSamNmTag(read, ref,
											leftMostIndex - 1));
						if (read.getAttribute(SAMTag.UQ.name()) != null)
							read.setAttribute(SAMTag.UQ.name(), SequenceUtil
									.sumQualitiesOfMismatches(read, ref,
											leftMostIndex - 1));
					} catch (Exception e) {
						throw new RuntimeException(e.toString());
					}
					if (read.getAttribute(SAMTag.MD.name()) != null)
						read.setAttribute(SAMTag.MD.name(), null);
				}
			}
		}
	}

	private void consensusAndRealigner(GaeaSamRecordBin readsBin) {
		final List<GaeaSamRecord> reads = readsBin.getReads();
		if (reads.size() == 0)
			return;

		byte[] reference = readsBin.getReference(chrInfo, parser);
		int leftMostIndex = readsBin.getLocation().getStart();

		// the reads cluster of perfectly match to reference
		final ArrayList<GaeaSamRecord> refReads = new ArrayList<GaeaSamRecord>();
		// the reads cluster that don't perfectly match to reference
		final ArrayList<GaeaAlignedSamRecord> altReads = new ArrayList<GaeaAlignedSamRecord>();
		// the reads cluster of making alternate consensus
		final LinkedList<GaeaAlignedSamRecord> readsForConsensus = new LinkedList<GaeaAlignedSamRecord>();

		consensusEngine.consensusByKnowIndels(knowIndelsSet, leftMostIndex,
				reference);
		long totalRawQuality = consensusEngine
				.consensusByReads(reads, refReads, altReads, readsForConsensus,
						leftMostIndex, reference);
		// function is empty
		consensusEngine.consensusBySmithWaterman();

		AlternateConsensus bestConsensus = consensusEngine
				.findBestAlternateConsensus(altReads, leftMostIndex);

		realignerCore(bestConsensus, altReads, reference, currentLocation,
				totalRawQuality, leftMostIndex);
	}

	private void realignerAndPending(ArrayList<VariantContext> knowIndels,
			GaeaSamRecord read, GenomeLocation location, RealignerWriter writer) {
		if (needRealignementReads.size() > 0) {
			consensusAndRealigner(needRealignementReads);
		}

		write(writer);

		do {
			currentInterval = iterator.hasNext() ? iterator.next() : null;
		} while (currentInterval != null
				&& (location == null || currentInterval.isBefore(location)));

		pending(knowIndels, read, writer);
	}

	private void pending(ArrayList<VariantContext> knowIndels,
			GaeaSamRecord read, RealignerWriter writer) {
		if (currentInterval == null) {
			notNeedRealignementReads.add(read);
			return;
		}
		if (read.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
			realignerAndPending(knowIndels, read, null, writer);
			return;
		}

		GenomeLocation location = parser.createGenomeLocation(read);
		if (location.getStop() == 0)
			location = parser.createGenomeLocation(location.getContig(),
					location.getStart(), location.getStart());
		if (location.isBefore(currentInterval)) {
			if (effectiveNotCleanReadCount != 0)
				effectiveNotCleanReadCount++;
			notNeedRealignementReads.add(read);
		} else if (location.overlaps(currentInterval)) {
			effectiveNotCleanReadCount++;

			if (RealignerFilter.needToClean(read, option.getMaxInsertSize())) {
				needRealignementReads.add(read);

				for (VariantContext variant : knowIndels) {
					if (!knowIndelsSet.contains(variant)) {
						knowIndelsSet.add(variant);
					}
				}
			} else {
				notNeedRealignementReads.add(read);
			}
			if (effectiveNotCleanReadCount >= MAX_READS) {
				write(writer);
				currentInterval = iterator.hasNext() ? iterator.next() : null;
			}
		} else {
			realignerAndPending(knowIndels, read, location, writer);
		}
	}

	private void write(RealignerWriter writer) {
		notNeedRealignementReads.addAll(needRealignementReads.getReads());
		writer.writeReadList(notNeedRealignementReads);
		needRealignementReads.clear();
		notNeedRealignementReads.clear();
		knowIndelsSet.clear();
		effectiveNotCleanReadCount = 0;
	}

	public void traversals(ArrayList<GaeaSamRecord> records,
			RealignerWriter writer) {
		updateWindowByInterval();

		ArrayList<VariantContext> overlapKnowIndels = null;

		for (GaeaSamRecord sam : records) {
			overlapKnowIndels = filterKnowIndels(sam);
			pending(overlapKnowIndels, sam, writer);
		}

		if (effectiveNotCleanReadCount > 0) {
			if (needRealignementReads.size() > 0) {
				consensusAndRealigner(needRealignementReads);
			}
			write(writer);
		}
	}
}
