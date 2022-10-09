package com.zto.fire.common.enu;

import org.apache.commons.lang3.StringUtils;

/**
 * SQL的操作类型
 *
 * @author ChengLong 2021年6月17日13:12:07
 * @since 2.0.0
 */
public enum Operation {
    SELECT(1), DROP_TABLE(2), RENAME_TABLE_OLD(3), RENAME_TABLE_NEW(4), CREATE_TABLE(5), CREATE_TABLE_AS_SELECT(6), CREATE_VIEW(7),
    REPLACE_TABLE(8), REPLACE_TABLE_AS_SELECT(9), RENAME_PARTITION_OLD(10), RENAME_PARTITION_NEW(11),
    DROP_PARTITION(12), TRUNCATE(13), CACHE(14), UNCACHE(15), REFRESH(16), CREATE_DATABASE(17), DROP_DATABASE(18),
    ADD_PARTITION(19), ALTER_TABLE(20), INSERT_INTO(21), INSERT_OVERWRITE(22), INSERT(23), SOURCE(24), SINK(25), GET(26), SCAN(27),
    ENABLE_TABLE(28), DISABLE_TABLE(29), DELETE(30), DELETE_FAMILY(31), DELETE_QUALIFIER(32), UPDATE(33), UNKNOWN(404);

    Operation(int type) {
    }

    /**
     * 将字符串解析成指定的枚举类型
     */
    public static Operation parse(String operation) {
        if (StringUtils.isBlank(operation)) return UNKNOWN;
        try {
            return Enum.valueOf(Operation.class, operation.trim().toUpperCase());
        } catch (Exception e) {
            return UNKNOWN;
        }
    }

}