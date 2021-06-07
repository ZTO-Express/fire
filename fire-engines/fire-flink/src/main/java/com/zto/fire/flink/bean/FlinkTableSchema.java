/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.flink.bean;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

/**
 * flink表模式，支持序列化
 * @author ChengLong 2020年1月16日 16:56:23
 */
public class FlinkTableSchema implements Serializable {
	private final String[] fieldNames;
	private final DataType[] fieldDataTypes;
	private final Map<String, Integer> fieldNameToIndex;

	public FlinkTableSchema(TableSchema schema) {
		this(schema.getFieldNames(), schema.getFieldDataTypes());
	}

	private FlinkTableSchema(String[] fieldNames, DataType[] fieldDataTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
		this.fieldDataTypes = Preconditions.checkNotNull(fieldDataTypes);

		fieldNameToIndex = new HashMap<>();
		final Set<String> duplicateNames = new HashSet<>();
		final Set<String> uniqueNames = new HashSet<>();
		for (int i = 0; i < fieldNames.length; i++) {
			// check for null
			Preconditions.checkNotNull(fieldDataTypes[i]);
			final String fieldName = Preconditions.checkNotNull(fieldNames[i]);

			// collect indices
			fieldNameToIndex.put(fieldName, i);

			// check uniqueness of field names
			if (uniqueNames.contains(fieldName)) {
				duplicateNames.add(fieldName);
			} else {
				uniqueNames.add(fieldName);
			}
		}
		if (!duplicateNames.isEmpty()) {
			throw new TableException(
				"Field names must be unique.\n" +
					"List of duplicate fields: " + duplicateNames.toString() + "\n" +
					"List of all fields: " + Arrays.toString(fieldNames));
		}
	}


	/**
	 * Returns all field data types as an array.
	 */
	public DataType[] getFieldDataTypes() {
		return fieldDataTypes;
	}

	/**
	 * This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataTypes()} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	public TypeInformation<?>[] getFieldTypes() {
		return fromDataTypeToLegacyInfo(fieldDataTypes);
	}

	/**
	 * Returns the specified data type for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<DataType> getFieldDataType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldDataTypes.length) {
			return Optional.empty();
		}
		return Optional.of(fieldDataTypes[fieldIndex]);
	}

	/**
	 * This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(int)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	public Optional<TypeInformation<?>> getFieldType(int fieldIndex) {
		return getFieldDataType(fieldIndex)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the specified data type for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<DataType> getFieldDataType(String fieldName) {
		if (fieldNameToIndex.containsKey(fieldName)) {
			return Optional.of(fieldDataTypes[fieldNameToIndex.get(fieldName)]);
		}
		return Optional.empty();
	}

	/**
	 * This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(String)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	public Optional<TypeInformation<?>> getFieldType(String fieldName) {
		return getFieldDataType(fieldName)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the number of fields.
	 */
	public int getFieldCount() {
		return fieldNames.length;
	}

	/**
	 * Returns all field names as an array.
	 */
	public String[] getFieldNames() {
		return fieldNames;
	}

	/**
	 * Returns the specified name for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<String> getFieldName(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldNames.length) {
			return Optional.empty();
		}
		return Optional.of(fieldNames[fieldIndex]);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(" |-- ").append(fieldNames[i]).append(": ").append(fieldDataTypes[i]).append('\n');
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FlinkTableSchema schema = (FlinkTableSchema) o;
		return Arrays.equals(fieldNames, schema.fieldNames) &&
			Arrays.equals(fieldDataTypes, schema.fieldDataTypes);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldDataTypes);
		return result;
	}
}
