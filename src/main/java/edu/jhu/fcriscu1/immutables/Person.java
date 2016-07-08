package edu.jhu.fcriscu1.immutables;

import org.immutables.value.Value;

/**
 * Created by fcriscuolo on 7/8/16.
 */
@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PRIVATE)
public interface Person {
    String getName();
    String getAddress();


}

