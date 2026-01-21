package org.github.dbjo.criteria.cache;

import java.util.List;
import java.util.Objects;
import org.github.dbjo.criteria.spec.*;

public final class SpecStringifier {
    private SpecStringifier() {}

    public static String stableKey(CondSpec c) {
        if (c == null) return "TRUE";
        if (c instanceof TrueSpec) return "T";
        if (c instanceof FalseSpec) return "F";

        if (c instanceof EqSpec e) return "EQ(" + e.property() + "," + valueKey(e.value()) + ")";
        if (c instanceof NeSpec e) return "NE(" + e.property() + "," + valueKey(e.value()) + ")";
        if (c instanceof IsNullSpec e) return "ISNULL(" + e.property() + ")";
        if (c instanceof IsNotNullSpec e) return "ISNOTNULL(" + e.property() + ")";
        if (c instanceof BetweenSpec e) return "BETWEEN(" + e.property() + "," + valueKey(e.lo()) + "," + valueKey(e.hi()) + ")";
        if (c instanceof CmpSpec e) return "CMP(" + e.property() + "," + e.op() + "," + valueKey(e.value()) + ")";
        if (c instanceof InSpec e) {
            StringBuilder sb = new StringBuilder("IN(").append(e.property()).append(",[");
            List<Object> vals = (e.values() == null) ? List.of() : e.values();
            for (int i=0;i<vals.size();i++) {
                if (i>0) sb.append(",");
                sb.append(valueKey(vals.get(i)));
            }
            return sb.append("])").toString();
        }
        if (c instanceof NotSpec e) return "NOT(" + stableKey(e.inner()) + ")";
        if (c instanceof AndSpec e) return "AND" + listKey(e.items());
        if (c instanceof OrSpec e)  return "OR" + listKey(e.items());

        return c.getClass().getSimpleName() + ":" + Objects.toString(c);
    }

    public static String stableKey(QuerySpec q) {
        StringBuilder sb = new StringBuilder();
        sb.append("E=").append(q.entityId()).append(";");
        sb.append("W=").append(stableKey(q.where())).append(";");
        if (q.scan() != null) {
            var r = q.scan().range();
            sb.append("S=").append(q.scan().property()).append(":")
                    .append(Objects.toString(r.lower())).append(":").append(r.lowerBound()).append(":")
                    .append(Objects.toString(r.upper())).append(":").append(r.upperBound()).append(";");
        } else {
            sb.append("S=NONE;");
        }
        sb.append("L=").append(q.limit() == null ? "NONE" : q.limit());
        return sb.toString();
    }

    static String listKey(List<CondSpec> items) {
        List<CondSpec> it = (items == null) ? List.of() : items;
        StringBuilder sb = new StringBuilder("[");
        for (int i=0;i<it.size();i++) {
            if (i>0) sb.append(",");
            sb.append(stableKey(it.get(i)));
        }
        return sb.append("]").toString();
    }

    public static String valueKey(Object v) {
        if (v == null) return "NULL";
        // For JSON transport you’d normalize Timestamp/Instant/enums here.
        return v.getClass().getName() + ":" + v.toString();
    }
}
