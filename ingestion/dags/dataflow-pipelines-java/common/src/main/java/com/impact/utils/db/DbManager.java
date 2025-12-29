package com.impact.utils.db;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class DbManager {

    private String host;
    private String port;
    private String user;
    private String password;
    private String database;
    protected String dbType;

    public DbManager(String host, String port, String user, String password, String database, String dbType) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = database;
        this.dbType = dbType;
    }

    public static String getDbDriver(String dbType) {
        switch (dbType.toLowerCase()) {
            case "postgresql":
                return "org.postgresql.Driver";
            case "mysql":
                return "com.mysql.cj.jdbc.Driver";
            case "oracle":
                return "oracle.jdbc.driver.OracleDriver";
            case "mssql":
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        return null;
    }

    public String getConnectionUrl() {
        switch (dbType.toLowerCase()) {
            case "postgresql":
                return String.format("jdbc:postgresql://%s:%s/%s", host, port, database);
            case "mysql":
                return String.format("jdbc:mysql://%s:%s/%s", host, port, database);
            case "oracle":
                return String.format("jdbc:oracle:thin:@//%s:%s/%s", host, port, database);
            case "mssql":
                return String.format("jdbc:sqlserver://%s:%s;database=%s;encrypt=true;trustServerCertificate=true;", host, port, database);
        }
        return null;
    }

    public void createTablev3(String tableId, Map<String, String> schema,
                              boolean replace, boolean createIfNotExists) throws Exception {
        // Pass tableId as full table namespace (ex: [DB].<SCHEMA_NAME>.<TABLE_NAME>)
        if(replace && createIfNotExists) {
            createIfNotExists = false;
        }
        // Creating column definitions string (ex: col1 STRING, col2 INT )
        StringBuilder columnDefinitions = new StringBuilder();
        String[] cols = schema.keySet().toArray(new String[0]);
        for(int i=0; i<cols.length; i++) {
            if(i!=cols.length-1) {
                columnDefinitions.append("\""+cols[i]+"\"").append(" ").append(schema.get(cols[i])).append(",\n");
            }
            else {
                columnDefinitions.append("\""+cols[i]+"\"").append(" ").append(schema.get(cols[i]));
            }
        }
        StringBuilder ddlQuery = new StringBuilder();
        if(dbType.equalsIgnoreCase("postgresql")) {
            ddlQuery =  new StringBuilder("CREATE UNLOGGED TABLE IF NOT EXISTS " + tableId + "(\n" + columnDefinitions + "\n) WITH (autovacuum_enabled=false);");
            if(replace) {
                // If replace is true, drop the table and create it. Make this an atomic operation
                ddlQuery = new StringBuilder(String.format("BEGIN;\nDROP TABLE IF EXISTS %s;\n", tableId)).append(ddlQuery)
                        .append("\nCOMMIT;");
            }
        }
        else if(dbType.equalsIgnoreCase("mssql")) {
            //ddlQuery =  new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableId + "(\n" + columnDefinitions + "\n);");
            if(replace) {
                // If replace is true, drop the table and create it. Make this an atomic operation
                ddlQuery = new StringBuilder(String.format("BEGIN TRANSACTION;\nDROP TABLE IF EXISTS %s;\n", tableId))
                        .append(new StringBuilder("CREATE TABLE " + tableId + "(\n" + columnDefinitions + "\n);"))
                        .append("\nCOMMIT TRANSACTION;");
            }
            else {
                ddlQuery = new StringBuilder(String.format("IF NOT EXISTS (\n SELECT 1 FROM sys.tables " +
                        "WHERE name='%s' AND schema_id=SCHEMA_ID('%s')\n)", "", ""));
                ddlQuery = ddlQuery.append(new StringBuilder("CREATE TABLE " + tableId + "(\n" + columnDefinitions + "\n);"));
            }
        }
        else {
            throw new Exception(String.format("dbType %s is not supported", dbType));
        }
        log.info(String.format("QUERY : %s", ddlQuery));
        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            Statement stmt = conn.createStatement();) {
            stmt.executeUpdate(String.valueOf(ddlQuery));
            log.info(String.format("Successfully created table %s", tableId));
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in DbManager createTable: %s", ExceptionUtils.getStackTrace(e)));
        }
    }

    public void createTable(String tableName, String schemaName, Map<String, String> schema, boolean replace, boolean createIfNotExists) throws SQLException {
        // Form the CREATE TABLE query
        String tableId = schemaName!=null ? schemaName+"."+tableName : tableName;
        if(replace && createIfNotExists) {
            createIfNotExists = false;
        }
        StringBuilder query = new StringBuilder("CREATE " + (replace ? " OR REPLACE " : "") + " UNLOGGED TABLE " + (createIfNotExists ? " IF NOT EXISTS " : "") + tableId + " (\n");
        Set<String> keys = schema.keySet();
        String[] cols = keys.toArray(new String[0]);
        for(int i=0; i<cols.length; i++) {
            if(i!=cols.length-1) {
                query.append("\""+cols[i]+"\"").append(" ").append(schema.get(cols[i])).append(",\n");
            }
            else {
                query.append("\""+cols[i]+"\"").append(" ").append(schema.get(cols[i]));
            }
        }
        query.append("\n) WITH (autovacuum_enabled=false)");
        log.info(String.format("QUERY : %s", query));

        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            Statement stmt = conn.createStatement();) {
            stmt.executeUpdate(String.valueOf(query));
            log.info(String.format("Successfully created table %s", tableId));
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in DbManager createTable: %s", ExceptionUtils.getStackTrace(e)));
        }
    }

    public String getInsertQuery(String tableName, String schemaName, Map<String, String> schema) {
        String tableId = schemaName!=null ? schemaName+"."+tableName : tableName;
        StringBuilder query = new StringBuilder("INSERT INTO " + tableId + "\n(");
        Set<String> keys = schema.keySet();
        String[] colNames = keys.toArray(new String[0]);
        String[] values = new String[colNames.length];
        for(int i=0; i<colNames.length; i++) {
            colNames[i] = "\""+colNames[i]+"\"";
            values[i] = "?";
        }
        query.append(StringUtils.join(colNames, ",")).append(")\n").append(" VALUES\n(");
        query.append(StringUtils.join(values, ",")).append(")");
        //String[] fields = new String[colNames.length];
        //Arrays.fill(fields, "?");
        //query.append(StringUtils.join(fields, ",")).append(")");
        return String.valueOf(query);
    }

    public Map<String, String> getTableSchema(String tableName, String schemaName) {
        String query = null;
        Map<String, String> schema = new LinkedHashMap<>();
        if("postgresql".equals(dbType)) {
            if(schemaName!=null) {
                query = String.format("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE table_catalog='%s' AND table_schema='%s' AND table_name='%s' ORDER BY ordinal_position",
                        database, schemaName, tableName);
            }
            else {
                query = String.format("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE table_catalog='%s' AND table_name='%s' ORDER BY ordinal_position",
                        database, tableName);
            }
        }
        log.info(String.format("Executing %s", query));
        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            Statement stmt = conn.createStatement();) {
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                schema.put(columnName, dataType);
            }
            return schema;
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in DbManager getTableSchema: %s", ExceptionUtils.getStackTrace(e)));
            return schema;
        }
    }

    public Map<String, String> getTableSchema(String query, String dbTable, String schemaName, String dbType, String bqTableName) {
        String schemaQuery = null;
        String emptyTableQuery = null;
        String dropQuery = null; 
        Map<String, String> schema = new LinkedHashMap<>();
        if(query.equals("")) {
            schema = getTableSchema(dbTable, schemaName, dbType);
            return schema;
        } else {
            if ("postgresql".equals(dbType)) {
                emptyTableQuery = String.format("SELECT * into public.tmp_schema_%s from " +
                    "(%s limit 0) tmp", bqTableName, query
                );
                schemaQuery = String.format("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE table_catalog='%s' AND table_schema='public' AND table_name='tmp_schema_%s' ORDER BY ordinal_position",
                database, bqTableName);
                dropQuery = String.format("DROP TABLE public.tmp_schema_%s",bqTableName);
            }
            try(Connection conn = DriverManager.getConnection(
                this.getConnectionUrl(), this.user, this.password
            ); Statement emptyTablestmt = conn.createStatement();) {
                log.info(String.format("Executing %s", emptyTableQuery));
                emptyTablestmt.executeUpdate(emptyTableQuery);
            }
            catch (Exception e) {
                log.error(String.format("Exception occured in DbManager:getQuerySchema:emptyTableQuery execution: %s", ExceptionUtils.getStackTrace(e)));
            }
            log.info(String.format("Empty table tmp_schema_%s created",bqTableName));
            try(Connection conn = DriverManager.getConnection(
                this.getConnectionUrl(), this.user, this.password
            ); Statement schemaQuerystmt = conn.createStatement();) {
                log.info(String.format("Executing %s", schemaQuery));
                ResultSet rs = schemaQuerystmt.executeQuery(schemaQuery);
                while(rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    schema.put(columnName, dataType);
                }
            }
            catch (Exception e) {
                log.error(String.format("Exception occurred in DbManager:getQuerySchema:schemaQuery execution: %s", ExceptionUtils.getStackTrace(e)));
                return schema;
            }
            try(Connection conn = DriverManager.getConnection(
                this.getConnectionUrl(), this.user, this.password
            ); Statement schemaQuerystmt = conn.createStatement();) {
                log.info(String.format("Executing %s", dropQuery));
                schemaQuerystmt.executeUpdate(dropQuery);
            }
            catch (Exception e) {
                log.error(String.format("Exception occurred in DbManager:getQuerySchema:dropQuery execution: %s", ExceptionUtils.getStackTrace(e)));
                return schema;
            }
            return schema;
        }
    }


    public Map<String, String> getTableSchema(String tableName, String schemaName, String dbType) {
        String query = null;
        Map<String, String> schema = new LinkedHashMap<>();
        if("postgresql".equals(dbType)) {
            if(schemaName!=null) {
                query = String.format("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE table_catalog='%s' AND table_schema='%s' AND table_name='%s' ORDER BY ordinal_position",
                        database, schemaName, tableName);
            }
            else {
                query = String.format("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE table_catalog='%s' AND table_name='%s' ORDER BY ordinal_position",
                        database, tableName);
            }
        }
        else if("mssql".equals(dbType)) {
            query = String.format("SELECT column_name, data_type FROM %s.INFORMATION_SCHEMA.COLUMNS " +
                            "WHERE table_catalog='%s' AND table_schema='%s' AND table_name='%s' ORDER BY ordinal_position",
                    database, database, schemaName, tableName);
        }
        else if("oracle".equals(dbType)) {
            query = String.format("SELECT column_name, data_type FROM ALL_TAB_COLUMNS " +
                            "WHERE table_name=upper('%s') ORDER BY column_id",
                     tableName);
        }
        log.info(String.format("Executing %s", query));
        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            Statement stmt = conn.createStatement();) {
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                schema.put(columnName, dataType);
            }
            return schema;
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in DbManager getTableSchema: %s", ExceptionUtils.getStackTrace(e)));
            return schema;
        }
    }
    public ResultSet executeQuery(String query) {
        ResultSet rs = null;
        log.info(String.format("Executing query:\n%s", query));
        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            PreparedStatement stmt = conn.prepareStatement(String.valueOf(query));) {
            Statement st = conn.createStatement();
            rs = st.executeQuery(query);
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in DbManager executeQuery: %s", ExceptionUtils.getStackTrace(e)));
        }
        return rs;
    }

    public void runQuery(String query) {
        log.info(String.format("Running query: \n%s", query));
        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            PreparedStatement stmt = conn.prepareStatement(String.valueOf(query));) {
                stmt.execute();
        }
        catch(Exception e) {
            log.error(String.format("Exception occurred in DbManager runQuery: %s", ExceptionUtils.getStackTrace(e)));
        }
    }

    public void testInsertQuery() {
        StringBuilder query = new StringBuilder("INSERT INTO store_validated_table_2022_10_19_test(\"SYNCSTARTDATETIME\") VALUES (?)");
        try(Connection conn = DriverManager.getConnection(this.getConnectionUrl(), this.user, this.password);
            PreparedStatement stmt = conn.prepareStatement(String.valueOf(query));) {
            stmt.setObject(1, "2022-10-14 04:36:31.070309", Types.TIMESTAMP);
            //stmt.executeUpdate(String.valueOf(query));
            stmt.execute();
            log.info(String.format("Successfully created table %s", "tableId"));
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in DbManager createTable: %s", ExceptionUtils.getStackTrace(e)));
        }
    }



}
