package com.impact.utils.snowflake;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.SnowflakeConnection;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeStatement;
import com.impact.utils.snowflake.SnowflakeSchema;

import java.sql.*;
import java.util.*;

@Slf4j
public class SnowflakeClient {
    private String serverName;
    private String userName;
    private String password;
    private String database;
    private String dbSchema;
    private String warehouse;
    private String accountIdentifier;
    private String userRole;

    private Connection connection;

    public static SnowflakeClient builder() {
        return new SnowflakeClient();
    }

    public SnowflakeClient withServer(String server) {
        this.serverName = server;
        return this;
    }

    public SnowflakeClient withAccountIdentifier(String accountIdentifier) {
        this.accountIdentifier = accountIdentifier;
        return this;
    }

    public SnowflakeClient withUsernamePasswordAuth(String userName, String password) {
        this.userName = userName;
        this.password = password;
        return this;
    }

    public SnowflakeClient withDatabase(String dbName) {
        this.database = dbName;
        return this;
    }

    public SnowflakeClient withDbSchema(String dbSchema) {
        this.dbSchema = dbSchema;
        return this;
    }

    public SnowflakeClient withWarehouse(String warehouse) {
        this.warehouse = warehouse;
        return this;
    }

    public SnowflakeClient withUserRole(String userRole) {
        this.userRole = userRole;
        return this;
    }

    public void createConnection() throws SQLException {
        Properties properties = new Properties();
        Preconditions.checkNotNull(serverName, "serverName cannot be null");
        Preconditions.checkNotNull(userName, "userName cannot be null");
        Preconditions.checkNotNull(password, "password cannot be null");
        properties.put("user", userName);
        properties.put("password", password);
        if(warehouse!=null) properties.put("warehouse", warehouse);
        if(database!=null) properties.put("db", database);
        if(dbSchema!=null) properties.put("schema", dbSchema);
        if(userRole!=null) {
            properties.put("role", userRole);
        }
        else {
            // If userRole is not provided, it'll try connecting with ACCOUNTADMIN privilege
            properties.put("role", "ACCOUNTADMIN");
        }
        String connectionStr = String.format("jdbc:snowflake://%s", serverName);
        log.info("Snowflake Connection String: " + connectionStr);
        this.connection = DriverManager.getConnection(connectionStr, properties);
    }

    public void closeConnection() throws SQLException {
        this.connection.close();
        this.connection = null;
    }

    public SnowflakeSchema getQueryResultSchema(String query) throws Exception {
        SnowflakeSchema sfSchema = null;
        //Map<Integer, SnowflakeSchemaField> snowflakeSchema = new LinkedHashMap<>();
        List<SnowflakeSchemaField> fields = new LinkedList<>();
        createConnection();
        Statement stmt =  connection.createStatement();
        ResultSet resultSet = stmt.unwrap(SnowflakeStatement.class).executeAsyncQuery(
                String.format("SELECT * FROM (%s) LIMIT 0", query)
        );
        QueryStatus queryStatus = QueryStatus.RUNNING;
        while (queryStatus == QueryStatus.RUNNING || queryStatus == QueryStatus.RESUMING_WAREHOUSE) {
            Thread.sleep(2000); // 2000 milliseconds.
            queryStatus = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
        }
        if (queryStatus == QueryStatus.FAILED_WITH_ERROR) {
            log.error("Error code: %d%n", queryStatus.getErrorCode());
            log.error("Error message: %s%n", queryStatus.getErrorMessage());
        }
        else if (queryStatus != QueryStatus.SUCCESS) {
            log.error("ERROR: unexpected QueryStatus: " + queryStatus);
        }
        else {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for(int colIdx=0 ; colIdx<resultSetMetaData.getColumnCount(); colIdx++) {
                fields.add(new SnowflakeSchemaField(
                        resultSetMetaData.getColumnName(colIdx+1),
                        resultSetMetaData.getColumnTypeName(colIdx+1)
                ));
                sfSchema = new SnowflakeSchema(fields);
            }
        }
        resultSet.close();
        stmt.close();
        closeConnection();
        return sfSchema;
    }

    public Map<String, String> convertSchemaToMap(SnowflakeSchema sfSchema)  {
        Map<String, String> schemaMap = new LinkedHashMap<>();
        for (SnowflakeSchemaField field : sfSchema.getFields()) {
            schemaMap.put(field.getName(), field.getType());
        }
        return schemaMap;
    }

    public void createTable(String database, String schema, String tableName, SnowflakeSchema tableSchema, String clusteringColumns, boolean replace) throws Exception {
        createConnection();
        try {
            StringBuilder query = new StringBuilder();
            StringBuilder drop_query = new StringBuilder();

            if (replace) {
                drop_query.append(String.format("DROP TABLE IF EXISTS %s.%s.%s;\n", database, schema, tableName));
                query.append(String.format("CREATE OR REPLACE TABLE %s.%s.%s\n(\n", database, schema, tableName));
            } else {
                query.append(String.format("CREATE TABLE IF NOT EXISTS %s.%s.%s\n(\n", database, schema, tableName));
            }

            for (int i = 0; i < tableSchema.getSize(); i++) {
                SnowflakeSchemaField field = tableSchema.getFields().get(i);
                query.append(String.format("\"%s\" %s", field.getName(), field.getType()));
                if (i < tableSchema.getSize() - 1) {
                    query.append(",\n");
                }
            }

            query.append("\n);");
            if (clusteringColumns != null && !clusteringColumns.equalsIgnoreCase("null")) {
                query.append(String.format("\n CLUSTER BY (%s);", clusteringColumns));
            }

            log.info("Executing Snowflake Drop DDL:\n" + drop_query);
            log.info("Executing Snowflake DDL:\n" + query);

            Statement statement = connection.createStatement();
            statement.execute(drop_query.toString());
            statement.execute(query.toString());
            statement.close();

        } catch (Exception e) {
            log.error("Error creating table in Snowflake: " + e.getMessage());
            throw e;
        } finally {
            closeConnection();
        }
    }

}
