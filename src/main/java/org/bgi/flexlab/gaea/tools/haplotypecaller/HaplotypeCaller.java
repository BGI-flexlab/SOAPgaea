package org.bgi.flexlab.gaea.tools.haplotypecaller;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bgi.flexlab.gaea.data.mapreduce.input.bed.RegionHdfsParser;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.haplotypecaller.assembly.ActivityProfileState;
import org.bgi.flexlab.gaea.tools.haplotypecaller.assembly.AssemblyRegion;
import org.bgi.flexlab.gaea.tools.haplotypecaller.downsampler.PositionalDownsampler;
import org.bgi.flexlab.gaea.tools.haplotypecaller.engine.HaplotypeCallerEngine;
import org.bgi.flexlab.gaea.tools.haplotypecaller.pileup.AssemblyRegionIterator;
import org.bgi.flexlab.gaea.tools.haplotypecaller.utils.RefMetaDataTracker;
import org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller.HaplotypeCallerOptions;
import org.bgi.flexlab.gaea.util.Window;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.VariantContext;

public class HaplotypeCaller {
	// intervals of windows
	private List<GenomeLocation> intervals = null;

	// hc engine
	private HaplotypeCallerEngine hcEngine = null;

	// options
	private HaplotypeCallerOptions options = null;

	// reference
	private ChromosomeInformationShare ref = null;

	// region
	private RegionHdfsParser region = null;

	// reads data source
	private ReadsDataSource readsSource = null;

	// sam file header
	private SAMFileHeader header = null;
	
	// assembly region output stream
	// for debug
	private PrintStream assemblyRegionOutStream = null;
	
	// activity profile output stream
	private PrintStream activityProfileOutStream = null;
	
	public static final int NO_INTERVAL_SHARDING = -1;

	private final List<LocalReadShard> shards = new ArrayList<>();
	
	private Shard<GaeaSamRecord> currentReadShard;

	private int maxProbPropagationDistance = 50;

	private double activeProbThreshold = 0.002;

	private int assemblyRegionPadding = 100;

	private int maxAssemblyRegionSize = 300;

	private int minAssemblyRegionSize = 50;
	
	private int maxReadsPerAlignmentStart = 0;

	public HaplotypeCaller(RegionHdfsParser region, Window win, HaplotypeCallerOptions options,
			ChromosomeInformationShare ref, Iterable<SAMRecordWritable> iterable, SAMFileHeader header) {
		this.options = options;
		this.ref = ref;
		this.region = region;
		this.header = header;
		this.readsSource = new ReadsDataSource(iterable, header);
		initializeIntervals(win);
	}

	public void dataSourceReset(Window win, Iterable<SAMRecordWritable> iterable) {
		if (readsSource != null)
			readsSource.dataReset(iterable);
		initializeIntervals(win);
	}

	private void initializeIntervals(Window win) {
		this.intervals = GenomeLocation.getGenomeLocationFormWindow(win, region);
	}

	private void makeReadsShard(int readShardSize, int readShardPadding) {
		for (final GenomeLocation interval : intervals) {
			if (readShardSize != NO_INTERVAL_SHARDING) {
				shards.addAll(LocalReadShard.divideIntervalIntoShards(interval, readShardSize, readShardPadding,
						readsSource, header.getSequenceDictionary()));
			} else {
				shards.add(new LocalReadShard(interval,
						interval.expandWithinContig(readShardPadding, header.getSequenceDictionary()), readsSource));
			}
		}
	}
	
	public final void traverse() {
        CountingReadFilter countedFilter = makeReadFilter();

        for ( final LocalReadShard readShard : shards ) {
            // Since reads in each shard are lazily fetched, we need to pass the filter to the window
            // instead of filtering the reads directly here
            readShard.setReadFilter(countedFilter);
            readShard.setDownsampler(maxReadsPerAlignmentStart > 0 ? new PositionalDownsampler(maxReadsPerAlignmentStart, header) : null);
            currentReadShard = readShard;

            processReadShard(readShard, features);
        }
    }

	private void processReadShard(Shard<GaeaSamRecord> shard, RefMetaDataTracker features) {
		final Iterator<AssemblyRegion> assemblyRegionIter = new AssemblyRegionIterator(shard, header, ref, features,
				hcEngine, minAssemblyRegionSize, maxAssemblyRegionSize, assemblyRegionPadding, activeProbThreshold,
				maxProbPropagationDistance, true);

		// Call into the tool implementation to process each assembly region
		// from this shard.
		while (assemblyRegionIter.hasNext()) {
			final AssemblyRegion assemblyRegion = assemblyRegionIter.next();
			writeAssemblyRegion(assemblyRegion);
			apply(assemblyRegion, features);
		}
	}

	private void writeAssemblyRegion(final AssemblyRegion region) {
		writeActivityProfile(region.getSupportingStates());

		if (assemblyRegionOutStream != null) {
			printIGVFormatRow(assemblyRegionOutStream,
					new GenomeLocation(region.getContig(), region.getStart(), region.getStart()), "end-marker", 0.0);
			printIGVFormatRow(assemblyRegionOutStream, region, "size=" + new GenomeLocation(region).size(),
					region.isActive() ? 1.0 : -1.0);
		}
	}

	private void writeActivityProfile(final List<ActivityProfileState> states) {
		if (activityProfileOutStream != null) {
			for (final ActivityProfileState state : states) {
				printIGVFormatRow(activityProfileOutStream, state.getLoc(), "state",
						Math.min(state.isActiveProb(), 1.0));
			}
		}
	}

	public List<VariantContext> apply(final AssemblyRegion region, final RefMetaDataTracker featureContext) {
		return hcEngine.callRegion(region, featureContext);
	}
	
	public void printIGVFormatRow(final PrintStream out, final Locatable loc, final String featureName, final double ... values) {
	    // note that start and stop are 0-based in IGV files, but the stop is exclusive so we don't subtract 1 from it
	    out.printf("%s\t%d\t%d\t%s", loc.getContig(), loc.getStart() - 1, loc.getEnd(), featureName);
	    for ( final double value : values ) {
	        out.print(String.format("\t%.5f", value));
	    }
	    out.println();
	}
}
