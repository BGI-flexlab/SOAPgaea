package org.bgi.flexlab.gaea.data.structure.location;

import org.bgi.flexlab.gaea.util.Window;

public class RealignerIntervalFilter extends GenomeLocationFilter {

	@Override
	public boolean filter(GenomeLocation location, Window win) {
		if (location.getStart() <= win.getStop() && location.getStart() >= win.getStart()
				|| location.getStop() >= win.getStart()
				&& location.getStop() <= win.getStop()) {
			return false;
		}
		return true;
	}
}
