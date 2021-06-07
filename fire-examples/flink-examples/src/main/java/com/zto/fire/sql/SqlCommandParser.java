/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlCommandParser {

	protected static final transient Logger logger = LoggerFactory.getLogger(SqlCommandParser.class);

	private SqlCommandParser() {}

	public static List<SqlCommandCall> parse(List<String> lines) {
		List<SqlCommandCall> calls = new ArrayList<>();
		StringBuilder stmt = new StringBuilder();
		for (String line : lines) {
			if (line.trim().isEmpty() || line.startsWith("--")) {
				// skip empty line and comment line
				continue;
			}
			stmt.append("\n").append(line);
			if (line.trim().endsWith(";")) {
				Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
				if (optionalCall.isPresent()) {
					calls.add(optionalCall.get());
				} else {
					throw new RuntimeException("Unsupported command '" + stmt.toString() + "'");
				}
				// clear string builder
				stmt.setLength(0);
			}
		}
		return calls;
	}

	public static Optional<SqlCommandCall> parse(String stmt) {
		// normalize
		stmt = stmt.trim();
		// remove ';' at the end
		if (stmt.endsWith(";")) {
			stmt = stmt.substring(0, stmt.length() - 1).trim();
		}

		// parse
		for (SqlCommand cmd : SqlCommand.values()) {
			final Matcher matcher = cmd.pattern.matcher(stmt);
			if (matcher.matches()) {
				final String[] groups = new String[matcher.groupCount()];
				for (int i = 0; i < groups.length; i++) {
					groups[i] = matcher.group(i + 1);
				}
				return cmd.operandConverter.apply(groups)
					.map((operands) -> new SqlCommandCall(cmd, operands));
			}
		}
		return Optional.empty();
	}

	private static final Function<String[], Optional<String[]>> NO_OPERANDS =
		(operands) -> Optional.of(new String[0]);

	private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
		(operands) -> Optional.of(new String[]{operands[0]});

	private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

	/**
	 * Supported SQL commands.
	 */
	public enum SqlCommand {
		INSERT_INTO(
			"(INSERT\\s+INTO.*)",
			SINGLE_OPERAND),

		CREATE_TABLE(
			"(CREATE\\s+TABLE.*)",
				SINGLE_OPERAND),
		CREATE_VIEW(
				"(CREATE\\s+VIEW.*)",
				SINGLE_OPERAND),

		SET(
			"SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
			(operands) -> {
				if (operands.length < 3) {
					return Optional.empty();
				} else if (operands[0] == null) {
					return Optional.of(new String[0]);
				}
				return Optional.of(new String[]{operands[1], operands[2]});
			});

		public final Pattern pattern;
		public final Function<String[], Optional<String[]>> operandConverter;

		SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
			this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
			this.operandConverter = operandConverter;
		}

		@Override
		public String toString() {
			return super.toString().replace('_', ' ');
		}

		public boolean hasOperands() {
			return operandConverter != NO_OPERANDS;
		}
	}

	/**
	 * Call of SQL command with operands and command type.
	 */
	public static class SqlCommandCall {
		public final SqlCommand command;
		public final String[] operands;

		public SqlCommandCall(SqlCommand command, String[] operands) {
			this.command = command;
			this.operands = operands;
		}

		public SqlCommandCall(SqlCommand command) {
			this(command, new String[0]);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SqlCommandCall that = (SqlCommandCall) o;
			return command == that.command && Arrays.equals(operands, that.operands);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(command);
			result = 31 * result + Arrays.hashCode(operands);
			return result;
		}

		@Override
		public String toString() {
			return command + "(" + Arrays.toString(operands) + ")";
		}
	}

	private static FileSystem getFiledSystem() throws IOException {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		return fileSystem;
	}

	public static void copyHdfsFileToLocal(String filePath, String disFile){

		logger.info("copy hdfs to local :" + filePath + ", hdfs:" + disFile);

		FSDataInputStream fsDataInputStream = null;
		try {
			File file = new File(disFile);
			if(file.exists()){
				file.delete();
				file = new File(disFile);
			}
			Path path = new Path(filePath);
			fsDataInputStream = getFiledSystem().open(path);
			IOUtils.copyBytes(fsDataInputStream, new FileOutputStream(file), 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fsDataInputStream != null){
				IOUtils.closeStream(fsDataInputStream);
			}
		}
	}

	private static void writeHDFS(String localPath, String hdfsPath){

		logger.info("copy file to hdfs :" + localPath + ", hdfs:" + hdfsPath);

		FSDataOutputStream outputStream = null;
		FileInputStream fileInputStream = null;

		try {
			Path path = new Path(hdfsPath);

			FileSystem fileSystem = getFiledSystem();
			if(fileSystem.exists(path)){
				fileSystem.delete(path);
			}

			outputStream = fileSystem.create(path);
			fileInputStream = new FileInputStream(new File(localPath));

			IOUtils.copyBytes(fileInputStream, outputStream,4096, false);

		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if(fileInputStream != null){
				IOUtils.closeStream(fileInputStream);
			}
			if(outputStream != null){
				IOUtils.closeStream(outputStream);
			}
		}
	}

	/**
	 * 接受sql文件，上传到hdfs，供run-application模式下载文件到本地，文件必须为绝对路径
	 * @param args
	 */
	public static void main(String[] args) {

		String sqlFile = args[0];

		logger.info("sqlFile:" + sqlFile);

		String path = "/tmp/" + sqlFile.substring(sqlFile.lastIndexOf("/") + 1);

		logger.info("path:" + path);

		writeHDFS(sqlFile, path);

	}

}
