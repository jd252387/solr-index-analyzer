package org.commrogue.analysis;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a selector for field names based on the {@code analysis.fl} request parameter.
 * When inactive, all fields are considered selected. When active, only the provided field
 * names are processed.
 */
public final class FieldSelector {
    private final Set<String> fieldNames;
    private final boolean active;

    public FieldSelector(Set<String> fieldNames, boolean active) {
        Objects.requireNonNull(fieldNames, "fieldNames");
        this.fieldNames = Collections.unmodifiableSet(new LinkedHashSet<>(fieldNames));
        this.active = active;
    }

    public static FieldSelector inactive() {
        return new FieldSelector(Set.of(), false);
    }

    public static FieldSelector ofActive(Set<String> fieldNames) {
        return new FieldSelector(fieldNames, true);
    }

    public boolean isActive() {
        return active;
    }

    public Set<String> fieldNames() {
        return fieldNames;
    }

    public boolean isEmpty() {
        return active && fieldNames.isEmpty();
    }

    public boolean includes(String fieldName) {
        if (!active) {
            return true;
        }
        return fieldNames.contains(fieldName);
    }
}
