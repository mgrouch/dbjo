package org.github.dbjo.meta.jdbc;

import java.sql.SQLType;

/**
 * Marker + shared defaults for metas that support temp-table batch upsert.
 *
 * IMPORTANT: this must be a CLASS (not interface) because DbMetaUpsertSupport is an abstract class.
 */
public abstract class DbMetaTempUpsertSupport<T> extends DbMetaUpsertSupport<T> {

    /**
     * Default temp insert uses the same params/types as single-row upsert MERGE.
     * Override if temp table differs from the MERGE parameter list.
     */
    public Object[] upsertTempInsertParams(T e) {
        return upsertByIdParams(e);
    }

    public SQLType[] upsertTempInsertParamTypes() {
        return upsertByIdParamTypes();
    }
}
