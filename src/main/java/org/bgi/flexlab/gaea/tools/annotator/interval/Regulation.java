package org.bgi.flexlab.gaea.tools.annotator.interval;

import org.bgi.flexlab.gaea.tools.annotator.effect.EffectType;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffects;

/**
 * Regulatory elements
 *
 * @author pablocingolani
 */
public class Regulation extends Marker {

	private static final long serialVersionUID = -5607588295343642199L;

	String cellType = "";
	String name = "";

	public Regulation() {
		super();
		type = EffectType.REGULATION;
	}

	public Regulation(Marker parent, int start, int end, boolean strandMinus, String id, String name, String cellType) {
		super(parent, start, end, strandMinus, id);
		type = EffectType.REGULATION;
		this.name = name;
		this.cellType = cellType;
	}

	@Override
	public Regulation cloneShallow() {
		Regulation clone = (Regulation) super.cloneShallow();
		clone.cellType = cellType;
		clone.name = name;
		return clone;
	}

	public String getCellType() {
		return cellType;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return getChromosomeName() + "\t" + start + "-" + end //
				+ " " //
				+ type + ((name != null) && (!name.isEmpty()) ? " '" + name + "'" : "");
	}

	/**
	 * Calculate the effect of this seqChange
	 */
	@Override
	public boolean variantEffect(Variant variant, VariantEffects variantEffects) {
		if (!intersects(variant)) return false; // Sanity check
		EffectType effType = EffectType.REGULATION;
		variantEffects.add(variant, this, effType, "");
		return true;
	}

}
