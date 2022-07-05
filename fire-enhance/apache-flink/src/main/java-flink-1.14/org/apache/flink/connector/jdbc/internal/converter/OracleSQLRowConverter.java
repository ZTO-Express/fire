package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * Oracle.
 */
public class OracleSQLRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Oracle";
    }

    public OracleSQLRowConverter(RowType rowType) {
        super(rowType);
    }
}
