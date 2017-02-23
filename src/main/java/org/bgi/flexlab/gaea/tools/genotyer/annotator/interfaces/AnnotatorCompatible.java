package org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces;


import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;

import java.util.List;


public interface AnnotatorCompatible {

    // getter methods for various used bindings
    public abstract VariantDataTracker getSnpEffRodBinding();
    public abstract VariantDataTracker getDbsnpRodBinding();
    public abstract List<VariantDataTracker> getCompRodBindings();
    public abstract List<VariantDataTracker> getResourceRodBindings();
    public abstract boolean alwaysAppendDbsnpId();
}
