package com.xjl.sql;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.internal.runners.model.EachTestNotifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
@Component
public class TableSqlProcessor implements ApplicationListener<ContextRefreshedEvent> {
	private static Log log = LogFactory.getLog(TableSqlProcessor.class);
	private ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
	@Autowired
	JdbcTemplate jdbcTemplate;
	    @Override  
	      public void onApplicationEvent(ContextRefreshedEvent event) {  
	        if(event.getApplicationContext().getParent() == null){  
	            String packageSearchPath = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX +
						resolveBasePackage("com.zxhy.xjl") + "/" + "**/*.sql";
	            try {
					Resource[] resources =  resourcePatternResolver.getResources(packageSearchPath);
					for (Resource resource : resources) {
						if (log.isDebugEnabled()){
							log.debug("find sql file:" + resource);
							log.debug("find sql fileName:" + resource.getFilename());
						}
						if (resource.isReadable()) {
							String sql = IOUtils.toString(resource.getInputStream());
							String tableFileName = resource.getURI().toString();
							int point = tableFileName.indexOf("!");
							if (point > 0){
					    		tableFileName = tableFileName.substring(point+1);
					    		String jarPath = resource.getURI().toString().substring(0, point);
					    		String jarFileName = FilenameUtils.getName(jarPath);
					    		tableFileName = jarFileName + "!" + tableFileName;
					    	}
							this.executeSQL(tableFileName, sql);
						}
					}
				} catch (IOException e) {
					log.error(e);
				}
	        }  
	      }  
	    protected String resolveBasePackage(String basePackage) {
	    	Environment environment = new StandardEnvironment();
			return ClassUtils.convertClassNameToResourcePath(environment.resolveRequiredPlaceholders(basePackage));
		}
	    protected boolean initSQLFile(String tableFileName){
	    	
	    	log.debug("tableFileName:" + tableFileName);
	    	String sql = "select * from xjl_table_log where sql_file_name=?";
	    	try {
	    		int rows = this.jdbcTemplate.update(sql, tableFileName);
	    		log.debug("数据库中的该文件已经存在的记录条数:" + rows);
	    		return rows > 0;
	    	} catch (RuntimeException e){
	    		System.out.println(e);
	    		String createTable = "create table xjl_table_log(sql_file_name varchar2(100), execute_date date)";
	    		this.jdbcTemplate.execute(createTable);
	    		return false;
	    	}
	    }
	    protected void finishSQLFile(String tableFileName){
	    	String sql = "insert into xjl_table_log(sql_file_name, execute_date) values(?,?)";
	    	Object[] args = new Object[2];
	    	args[0] = tableFileName;
	    	args[1] = new Date();
	    	this.jdbcTemplate.update(sql, args);
	    }
	    //执行sql
	    protected void executeSQL(String tableFileName, String sql){
	    	String[] batchSql = sql.split(";");
	    	List<String> sqlAll = new ArrayList<>();
	    	for (String sqlOne : batchSql) {
				if (org.apache.commons.lang3.StringUtils.isBlank(sqlOne)){
					continue;
				} else {
					sqlAll.add(org.apache.commons.lang3.StringUtils.trim(sqlOne));
				}
			}
	    	if (sqlAll.isEmpty()){
	    		return;
	    	}
	    	this.executeSQL(tableFileName, sqlAll.toArray(new String[0]));
	    }
	    protected void executeSQL(String tableFileName, String[] sql){
	    	if (this.initSQLFile(tableFileName)){
	    		log.debug("该文件已经被初始化过，不再执行该SQL");
	    		return;
	    	}
	    	for (String sqlOne : sql) {
				log.debug("sql:" + sqlOne);
			}
	    	this.jdbcTemplate.batchUpdate(sql);
	    	this.finishSQLFile(tableFileName);
	    }
}
