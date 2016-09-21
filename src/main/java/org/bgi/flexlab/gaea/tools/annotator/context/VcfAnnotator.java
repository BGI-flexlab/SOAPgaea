package org.bgi.flexlab.gaea.tools.annotator.context;

import org.bgi.flexlab.gaea.tools.annotator.conf.Config;

/**
 * Annotate a VCF file: E.g. add information to INFO column
 *
 */
public interface VcfAnnotator {

	/**
	 * Add annotation headers to VCF file
	 *
	 * @return true if OK, false on error
	 */
	public boolean addHeaders(VcfFileIterator vcfFile);

	/**
	 * Annotate a VCF file entry
	 *
	 * @return true if the entry was annotated
	 */
	public boolean annotate(VcfEntry vcfEntry);

	/**
	 * This method is called after all annotations have been performed.
	 * The vcfFile might have already been closed by this time
	 * (i.e. the VcfFileIterator reached the end).
	 *
	 * @return true if OK, false on error
	 */
	public boolean annotateFinish();

	/**
	 * Initialize annotator: This method is called after vcfFile
	 * is opened, but before the header is output.
	 * The first vcfEntry might have (and often has) already been
	 * read from the file.
	 *
	 * @return true if OK, false on error
	 */
	public boolean annotateInit(VcfFileIterator vcfFile);

	/**
	 * Set configuration
	 */
	public void setConfig(Config config);

	/**
	 * Set debug mode
	 */
	public void setDebug(boolean debug);

	/**
	 * Set verbose mode
	 */
	public void setVerbose(boolean verbose);
}
