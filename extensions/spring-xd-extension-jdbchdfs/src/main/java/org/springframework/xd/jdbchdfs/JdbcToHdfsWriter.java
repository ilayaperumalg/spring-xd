/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.xd.jdbchdfs;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.AbstractCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.DatabaseType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.xd.batch.item.hadoop.HdfsTextItemWriter;
import org.springframework.xd.jdbc.NamedColumnJdbcItemReader;
import org.springframework.xd.jdbc.NamedColumnJdbcReader;
import org.springframework.xd.tuple.Tuple;

/**
 * @author Ilayaperumal Gopinathan
 */
public class JdbcToHdfsWriter implements Tasklet, InitializingBean {

	private static final Log log = LogFactory.getLog(JdbcToHdfsWriter.class);

	private DataSource dataSource;

	private String tableName;

	private String columnNames;

	private String partitionClause;

	private String sql;

	private int fetchSize;

	private boolean verifyCursorPosition = true;

	private NamedColumnJdbcReader reader;

	private final HdfsTextItemWriter hdfsTextItemWriter;

	public JdbcToHdfsWriter(HdfsTextItemWriter hdfsTextItemWriter) {
		this.hdfsTextItemWriter = hdfsTextItemWriter;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		ExecutionContext executionContext = chunkContext.getStepContext().getStepExecution().getExecutionContext();
		NamedColumnJdbcItemReader itemReader = reader.getReader();
		itemReader.open(executionContext);
		Tuple tuple = itemReader.read();
		List<Tuple> items = new ArrayList<Tuple>();
		items.add(tuple);
		hdfsTextItemWriter.write(items);
		hdfsTextItemWriter.close();
		itemReader.close();
		return RepeatStatus.FINISHED;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		reader = new NamedColumnJdbcReader();
		if (!StringUtils.hasText(sql)) {
			Assert.hasText(tableName, "tableName must be set");
			Assert.hasText(columnNames, "columns must be set");

			String sql;
			if (StringUtils.hasText(partitionClause)) {
				sql = "SELECT " + columnNames + " FROM " + tableName + " " + partitionClause;
			}
			else {
				sql = "SELECT " + columnNames + " FROM " + tableName;
			}
			log.info("Setting SQL to: " + sql);
			setSql(sql);
		}
		else if (StringUtils.hasText(columnNames) || StringUtils.hasText(tableName)) {
			log.warn("You must set either the 'sql' property or 'tableName' and 'columns'.");
		}

		DatabaseType type = DatabaseType.fromMetaData(dataSource);

		switch (type) {
			case MYSQL:
				fetchSize = Integer.MIN_VALUE;
				// MySql doesn't support getRow for a streaming cursor
				verifyCursorPosition = false;
				break;
			case SQLITE:
				fetchSize = AbstractCursorItemReader.VALUE_NOT_SET;
				break;
			default:
				// keep configured fetchSize
		}
		reader.setSql(sql);
		reader.setFetchSize(fetchSize);
		reader.setDataSource(dataSource);
		reader.setVerifyCursorPosition(verifyCursorPosition);
		reader.afterPropertiesSet();
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setColumnNames(String columnNames) {
		this.columnNames = columnNames;
	}

	public void setPartitionClause(String partitionClause) {
		this.partitionClause = partitionClause;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

}
