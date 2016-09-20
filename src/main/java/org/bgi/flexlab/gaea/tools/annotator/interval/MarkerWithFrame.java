package org.bgi.flexlab.gaea.tools.annotator.interval;

/**
 * A Marker that has 'frame' information (Exon and Cds)
 * 
 * @author pcingola
 */
public interface MarkerWithFrame {

	public int getFrame();

	public void setFrame(int frame);

}
