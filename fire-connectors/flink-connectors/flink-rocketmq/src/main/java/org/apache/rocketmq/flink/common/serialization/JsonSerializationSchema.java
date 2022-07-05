package org.apache.rocketmq.flink.common.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.rocketmq.common.message.Message;

import javax.annotation.Nullable;

/**
 * 将RowData序列化成rocketmq消息
 * @author ChengLong 2021-5-9 13:40:17
 */
public class JsonSerializationSchema implements TagKeyValueSerializationSchema<RowData> {

    private final String topic;

    private final @Nullable String tags;

    private final SerializationSchema<RowData> valueSerialization;

    private RowData.FieldGetter[] keyFieldGetters;

    private RowData.FieldGetter[] valueFieldGetters;

    public JsonSerializationSchema(
            String topic,
            @Nullable String tags,
            SerializationSchema<RowData> valueSerialization) {
        this.topic = topic;
        this.tags = tags;
        this.valueSerialization = valueSerialization;
    }

    public JsonSerializationSchema(
            String topic,
            @Nullable String tags,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters) {
        this.topic = topic;
        this.tags = tags;
        this.valueSerialization = valueSerialization;
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        valueSerialization.open(context);
    }

    @Override
    public Message serialize(RowData consumedRow) {
        final byte[] valueSerialized = valueSerialization.serialize(consumedRow);
        return new Message(
                topic,
                tags,
                null,
                valueSerialized);
    }
}
