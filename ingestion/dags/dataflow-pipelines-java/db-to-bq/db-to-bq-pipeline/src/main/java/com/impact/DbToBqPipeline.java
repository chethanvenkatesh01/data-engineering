package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.bigquery.BigQueryClient;
import com.impact.utils.bigquery.BigQuerySchemaConversion;
import com.impact.utils.db.DbManager;
import com.impact.utils.gcp.SecretManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Slf4j
public class DbToBqPipeline {

    public static List<String> numericTypes = Arrays.asList("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "float", "double", "decimal",
            "numeric", "real", "double precision");
    public static List<String> dateTypes = Arrays.asList("date");
    public static List<String> datetimeTypes = Arrays.asList("datetime","timestamp","datetime2","smalldatetime","timestamptz");

    public static List<String> stringTypes = Arrays.asList("string", "text", "varchar", "char", "character varying",
            "character");

    public static void main(String [] args) throws Exception {
        DbToBqPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DbToBqPipelineOptions.class);
        run(options);
    }

    private static void run(DbToBqPipelineOptions options) throws Exception {
        // BQ Params
        String project = options.as(GcpOptions.class).getProject();
        String bqBillingProject = options.getBigqueryBillingProject();
        String bqDataset = options.getBigqueryDataset();
        String bqTable = options.getBigqueryTable();
        String bqRegion = options.getBigqueryRegion();
        if(bqRegion==null) {
            bqRegion = options.as(GcpOptions.class).getWorkerRegion();
        }
        // DB params
        String host = null;
        String port = null;
        String user = null;
        String password = null;
        String db = null;
        String schemaName = null;
        boolean replaceSpecialChars = options.getReplaceSpecialChars();
        String secretName = options.getDbSecretName();
        Integer maxPartitionSize = options.getMaxPartitionSize();
        String auditColumnName = options.getAuditColumn();

        host = options.getDbHost();
        port = options.getDbPort();
        user = options.getDbUser();
        password = options.getDbPassword();
        db = options.getDbName();
        schemaName = options.getDbSchemaName();


        if(secretName!=null) {
            log.info("Fetching the credentials from secert manager");
            log.info(String.format("Secret name %s", secretName));
            String secret = SecretManager.getSecret(project, secretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);

            for(String key : config.keySet()) {
                if(key.contains("host") || key.contains("server")) host = config.get(key).getAsString();
                else if(key.contains("port")) port = config.get(key).getAsString();
                else if(key.contains("user")) user = config.get(key).getAsString();
                else if(key.contains("password")) password = config.get(key).getAsString();
                else if(key.contains("database") || key.contains("db")) db = config.get(key).getAsString();
                else if(key.contains("schema") && schemaName==null) schemaName = config.get(key).getAsString();
            }
            log.info(String.format("Host: %s;Port: %s;Database: %s", host, port, db));
        }

        String dbTable = options.getDbTable();
        String queriesString = options.getSourceQueries();
        log.info(String.format("Queries %s", queriesString));

        String[] queries = queriesString!=null ? queriesString.split(";") : new String[0];
        log.info("##################################");
        log.info(String.valueOf(queries.length));
        log.info("##################################");
        String pullType = options.getPullType();
        String dbPartitionColumn = options.getDbPartitionColumn()==null || "null".equals(options.getDbPartitionColumn().toLowerCase())
                ? null : options.getDbPartitionColumn();
        Integer numPartitions = options.getNumPartitions()==null || "null".equals(String.valueOf(options.getNumPartitions()).toLowerCase())
                ? null : options.getNumPartitions();
//        String dbIncrementalColumn = options.getDbIncrementalColumn()==null || "null".equals(options.getDbIncrementalColumn().toLowerCase())
//                ? null : options.getDbIncrementalColumn();
//        String dbIncrementalColumnValue = options.getDbIncrementalColumnValue()==null || "null".equals(options.getDbIncrementalColumnValue().toLowerCase())
//                ? null : options.getDbIncrementalColumnValue();
        String dbIncrementalColumn = options.getIncrementalColumn()==null || "null".equals(options.getIncrementalColumn().toLowerCase())
                || "none".equals(options.getIncrementalColumn().toLowerCase())
                ? null : options.getIncrementalColumn();
        String dbIncrementalColumnValue = options.getIncrementalColumnValue()==null || "null".equals(options.getIncrementalColumnValue().toLowerCase())
                ? null : options.getIncrementalColumnValue();
        Integer queryPartitioningThreshold = options.getQueryPartitioningThreshold()==null || "null".equals(String.valueOf(options.getQueryPartitioningThreshold()).toLowerCase())
                ? null : options.getQueryPartitioningThreshold();

        // ------------------------- Validate that one among queries or dbTable is passed -----------------
        if(queries.length == 0 && dbTable.equals("")) {
            throw new Exception("Both dbTable and queries Array cannot be empty");
        }

        // -------------------------  Validate DB Type and create full table name -------------------------
        String dbType = null;
        String tableId = null;
        if("postgresql".equals(options.getDbType().toLowerCase()) || "postgres".equals(options.getDbType().toLowerCase())
        ||"pg".equals(options.getDbType().toLowerCase() )) {
            dbType = "postgresql";
            tableId = String.format("%s.%s", schemaName, dbTable);
            port = port!=null ? port : "5432";
        }
        else if("mysql".equals(options.getDbType().toLowerCase())) {
            dbType = "mysql";
            tableId = String.format("%s.%s", schemaName, dbTable);
            port = port!=null ? port : "3306";
        }
        else if("mssql".equals(options.getDbType().toLowerCase()) || "sqlserver".equals(options.getDbType().toLowerCase())) {
            dbType = "mssql";
            tableId = String.format("%s.%s.%s", db, schemaName, dbTable);
            port = port!=null ? port : "1433";
        }
        else if("oracle".equals(options.getDbType().toLowerCase())) {
            dbType = "oracle";
            tableId = String.format("%s.%s", schemaName, dbTable);
            port = port != null ? port : "1521";
        }
        else {
            log.error(String.format("dbType %s is not supported", options.getDbType()));
            throw new Exception(String.format("dbType %s is not supported", options.getDbType()));
        }

        // ------------------------- Creating DbManager instance  -------------------------
        assert host!=null : "host is null";
        assert user!=null : "user is null";
        assert password!=null : "password is null";
        DbManager dbManager = new DbManager(host, port, user, password, db, dbType);
        String driverClassName = DbManager.getDbDriver(dbType);
        String connectionUrl = dbManager.getConnectionUrl();
        log.info(String.format("Driver: %s\nConnection Url: %s", driverClassName, connectionUrl));
        Pipeline p = Pipeline.create(options);

        if(queries.length > 0 && !queries[0].equals("")) {
            log.info("$$$$$$$$$$$$$$$$$$$$$$$$$$");
            String firstQuery = queries[0];
            // Enter this if condition if queries are passed. This will ignore dbTable and its other relevant parameters

            log.info(String.format("Length of queries array is > 0. firstQuery: %s", firstQuery));
            // -------------------------  Creating schemas -------------------------
            Map<String, String> dbSchema = dbManager.getTableSchema(firstQuery, dbTable, schemaName, dbType, bqTable);
            Map<String, String> bqSchema = BigQuerySchemaConversion.dbToBq(dbSchema, dbType);
            TableSchema bqTableSchema = BigQuerySchemaConversion.convertMapToTableSchema(bqSchema);
            bqBillingProject = bqBillingProject!=null ? bqBillingProject : project;
            BigQueryClient bqClient = new BigQueryClient(bqBillingProject, bqRegion);
            bqClient.createTable(project,bqDataset,bqTable,bqTableSchema,options.getBqPartitionColumn(),options.getBqClusteringColumns(),
                    options.getReplaceTable());
            for(int i=0; i<queries.length; i++) {
                p.apply(String.format("Read from DB part-%s", i+1), JdbcIO.<TableRow>read().withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl).withUsername(user).withPassword(password))
                        .withQuery(queries[i])
                        .withRowMapper(new MapToTableRow(dbSchema, bqSchema, dbType, replaceSpecialChars)))
                .apply(String.format("Write to BigQuery part-%s", i+1), BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", project, bqDataset, bqTable))
                        .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
            }

        } else {
            // Enter this condition if queries are not passed and dbTable is passed.
            // ------------------------- Creating Base Query  -------------------------
            //String baseQuery = String.format("SELECT * FROM %s", tableId);
            String baseQuery = getBaseQuery(tableId, dbType, dbIncrementalColumn, auditColumnName, options.getReplaceTable());


            // ------------------------- Build incremental filter if required  -------------------------
            String incrementalFilter = null;
            if("incremental".equals(pullType.toLowerCase())) {
                if(dbIncrementalColumn!=null) {
                    if(dbIncrementalColumnValue!=null) {
                        log.info(String.format("Provided value for dbIncrementalColumnValue : %s", dbIncrementalColumnValue));
                        String incrementalColTypeQuery = String.format("SELECT DATA_TYPE AS incremental_column_datatype FROM INFORMATION_SCHEMA.COLUMNS " +
                                " WHERE LOWER(TABLE_NAME)='%s'" +
                                " AND LOWER(TABLE_SCHEMA)='%s'" +
                                " AND LOWER(COLUMN_NAME)='%s'", dbTable.toLowerCase(), schemaName.toLowerCase(), dbIncrementalColumn.toLowerCase());
                        log.info(String.format("incrementalColTypeQuery: \n %s", incrementalColTypeQuery));
                        String incrementalColDataType = null;
                        ResultSet incrementalColTypeQueryResults = null;
                        try(Connection conn = DriverManager.getConnection(connectionUrl, user, password);
                            PreparedStatement stmt = conn.prepareStatement(incrementalColTypeQuery);) {
                            Statement st = conn.createStatement();
                            incrementalColTypeQueryResults = st.executeQuery(incrementalColTypeQuery);
                            if(incrementalColTypeQueryResults!=null && incrementalColTypeQueryResults.next()) {
                                incrementalColDataType = incrementalColTypeQueryResults.getString("incremental_column_datatype");
                            }
                        }
                        catch (Exception e) {
                            throw e;
                        }
                        if(numericTypes.contains(incrementalColDataType.toLowerCase())) {
                            incrementalFilter = String.format("%s > %s", dbIncrementalColumn, dbIncrementalColumnValue);
                        }
                        else {
                            incrementalFilter = String.format("%s > '%s'", dbIncrementalColumn, dbIncrementalColumnValue);
                        }
                    }
                    else {
                        throw new Exception("dbIncrementalColumnValue is null");
                    }
                }
                else {
                    throw new Exception(String.format("pullType is %s but dbIncrementalColumn is null", pullType.toLowerCase()));
                }
            }
            else {
                incrementalFilter = "";
            }


            // -------------------------  Validate if query partitioning is required -------------------------
            ResultSet rowCount = null;
            boolean requireQueryPartitioning = false;
            String rowCountQuery = String.format("SELECT COUNT(1) AS row_count FROM %s", tableId) +
                    (incrementalFilter.length()>1 ? String.format(" WHERE %s", incrementalFilter) : "");
            log.info(String.format("Executing query:\n%s",rowCountQuery));
            try(Connection conn = DriverManager.getConnection(connectionUrl, user, password);
                PreparedStatement stmt = conn.prepareStatement(rowCountQuery);) {
                Statement st = conn.createStatement();
                rowCount = st.executeQuery(rowCountQuery);
                if(rowCount!=null && rowCount.next()) {
                    int numRows = rowCount.getInt("row_count");
                    log.info(String.format("Row count: %s", numRows));
                    if(queryPartitioningThreshold==null) {
                        log.info(String.format("queryPartitioningThreshold is null. Hence setting requireQueryPartitioning=false"));
                        requireQueryPartitioning = false;
                    }
                    else if(numRows==0) {
                        log.info(String.format("%s table is empty! Terminating the pipeline", tableId));
                        return;
                    }
                    else if(numRows>queryPartitioningThreshold) {
                        log.info(String.format("Setting the requireQueryPartition to true as row count (%s) is greater than queryPartitioningThreshold (%s)",
                                numRows, queryPartitioningThreshold));
                        requireQueryPartitioning = true;
                    }
                    else {
                        log.info(String.format("Setting the requireQueryPartition to false as row count (%s) is less than queryPartitioningThreshold (%s)",
                                numRows, queryPartitioningThreshold));
                        requireQueryPartitioning = false;
                    }
                }
                else {
                    log.info("Unexpected error occured. Either rowCount is null or rowCount.next() is false");
                }
            }
            catch (Exception e) {
                log.error(String.format("Exception occured: ", ExceptionUtils.getStackTrace(e)));
            }

            log.info(String.format("requireQueryPartitioning = %s", requireQueryPartitioning));

            // -------------------------  Creating schemas -------------------------

            Map<String, String> dbSchema = dbManager.getTableSchema(dbTable, schemaName, dbType.toLowerCase());
            if(auditColumnName!=null && !options.getReplaceTable() && (dbIncrementalColumn==null || !dbIncrementalColumn.equalsIgnoreCase(auditColumnName))) {
                dbSchema.put(auditColumnName, "DATETIME");
            }

            Map<String, String> bqSchema = BigQuerySchemaConversion.dbToBq(dbSchema, dbType);
            TableSchema bqTableSchema = BigQuerySchemaConversion.convertMapToTableSchema(bqSchema);
            bqBillingProject = bqBillingProject!=null ? bqBillingProject : project;
            BigQueryClient bqClient = new BigQueryClient(bqBillingProject, bqRegion);
            bqClient.createTable(project,bqDataset,bqTable,bqTableSchema,options.getBqPartitionColumn(),options.getBqClusteringColumns(),
                    options.getReplaceTable());

            String finalQuery = null;

            
            //PCollection<byte[]> start = p.apply("Start", Impulse.create());

            if(requireQueryPartitioning) {
                log.info("REQUIRES QUERY PARTITIONING");
                if(dbPartitionColumn!=null) {
                    log.info("Validating the partition column");
                    String partitionColumnDType = null;
                    ResultSet columnsMetadataResultset = null;
                    String columnsMetadataQuery = String.format("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                            "WHERE LOWER(TABLE_NAME)='%s'" +
                            " AND LOWER(TABLE_SCHEMA)='%s'" +
                            " AND LOWER(COLUMN_NAME)='%s'", dbTable.toLowerCase(), schemaName.toLowerCase(), dbPartitionColumn.toLowerCase());
                    log.info(String.format("columnsMetadataQuery: \n %s", columnsMetadataQuery));
                    try(Connection conn = DriverManager.getConnection(connectionUrl, user, password);
                        PreparedStatement stmt = conn.prepareStatement(columnsMetadataQuery);) {
                        Statement st = conn.createStatement();
                        columnsMetadataResultset = st.executeQuery(columnsMetadataQuery);
                        if(columnsMetadataResultset.next()) {
                            partitionColumnDType = columnsMetadataResultset.getString("DATA_TYPE");
                            boolean isPartitionColValid = validatePartitionColumn(partitionColumnDType.toLowerCase());
                            if(isPartitionColValid &&
                                    (numericTypes.contains(partitionColumnDType) || dateTypes.contains(partitionColumnDType)
                                    || datetimeTypes.contains(partitionColumnDType))) {
                                log.info(String.format("%s is a valid partition column. Determining the query bounds", dbPartitionColumn));
                                String boundsQuery = String.format("SELECT MIN(%s) AS lower_bound,MAX(%s) AS upper_bound FROM %s",
                                        dbPartitionColumn, dbPartitionColumn, tableId);
                                log.info(String.format("Query for determining bounds: \n%s", boundsQuery));
                                ResultSet partitionColumnBoundsResultset = st.executeQuery(boundsQuery);
                                if(partitionColumnBoundsResultset.next()) {
                                    String lowerBound = partitionColumnBoundsResultset.getString("lower_bound");
                                    String upperBound = partitionColumnBoundsResultset.getString("upper_bound");
                                    List<List<String>> queryPartitions = generatePartitionBoundaries(lowerBound, upperBound, partitionColumnDType.toLowerCase(), numPartitions);
                                    log.info(String.format("Query partitions: %s", queryPartitions));
                                    assert queryPartitions.size()>0 : "queryPartitions is empty";
                                    String partitionFilter = (dbPartitionColumn+">=") + (numericTypes.contains(partitionColumnDType.toLowerCase()) ? "%s" : "'%s'")
                                            + " AND " + (dbPartitionColumn+"<") + (numericTypes.contains(partitionColumnDType.toLowerCase()) ? "%s" : "'%s'");
                                    String lastPartitionFilter = (dbPartitionColumn+">=") + (numericTypes.contains(partitionColumnDType.toLowerCase()) ? "%s" : "'%s'")
                                            + " AND " + (dbPartitionColumn+"<=") + (numericTypes.contains(partitionColumnDType.toLowerCase()) ? "%s" : "'%s'");
                                    for(int i=0; i<queryPartitions.size(); i++) {
                                        if(i==queryPartitions.size()-1) {
                                            if(incrementalFilter.length()>1) {
                                                finalQuery = baseQuery + String.format("WHERE %s AND ", incrementalFilter) +
                                                        String.format(lastPartitionFilter,queryPartitions.get(i).get(0).replace("T"," "),
                                                                queryPartitions.get(i).get(1).replace("T"," "));
                                            }
                                            else {
                                                finalQuery = baseQuery + " WHERE " +
                                                        String.format(lastPartitionFilter,queryPartitions.get(i).get(0).replace("T"," "),
                                                                queryPartitions.get(i).get(1).replace("T"," "));
                                            }
                                        }
                                        else {
                                            if(incrementalFilter.length()>1) {
                                                finalQuery = baseQuery + String.format("WHERE %s AND ", incrementalFilter) +
                                                        String.format(partitionFilter,queryPartitions.get(i).get(0).replace("T"," "),
                                                                queryPartitions.get(i).get(1).replace("T"," "));
                                            }
                                            else {
                                                finalQuery = baseQuery + " WHERE " +
                                                        String.format(partitionFilter,queryPartitions.get(i).get(0).replace("T"," "),
                                                                queryPartitions.get(i).get(1).replace("T"," "));
                                            }
                                        }
                                        log.info(String.format("Final Query %s : %s", i, finalQuery));
                                        p.apply(String.format("Read from DB part-%s", i+1), JdbcIO.<TableRow>read().withDataSourceConfiguration(
                                                                JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl).withUsername(user).withPassword(password))
                                                        .withQuery(finalQuery)
                                                        .withRowMapper(new MapToTableRow(dbSchema, bqSchema, dbType, replaceSpecialChars)))
                                                .apply(String.format("Write to BigQuery part-%s", i+1), BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", project, bqDataset, bqTable))
                                                        .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
                                    }
                                }
                                else {
                                    throw new Exception(String.format("partitionColumnBoundsResultset.next() is false"));
                                }
                            }
                            else if(isPartitionColValid && stringTypes.contains(partitionColumnDType)) {
                                log.info(String.format("%s is a valid partition column. Determining the query bounds", dbPartitionColumn));
                                String boundsQuery = String.format("SELECT %s, COUNT(1) AS cnt FROM %s %s GROUP BY %s ORDER BY cnt",
                                        dbPartitionColumn,
                                        tableId,
                                        incrementalFilter.length()>1 ? "WHERE " + incrementalFilter : "",
                                        dbPartitionColumn);
                                log.info(String.format("Query for determining bounds: \n%s", boundsQuery));
                                ResultSet partitionColumnBoundsResultset = st.executeQuery(boundsQuery);
                                Map<String, Integer> dbPartitionColumnValuesCount = new LinkedHashMap<>();
                                while(partitionColumnBoundsResultset.next()) {
                                    dbPartitionColumnValuesCount.put(partitionColumnBoundsResultset.getString(dbPartitionColumn),
                                            partitionColumnBoundsResultset.getInt("cnt"));
                                }
                                log.info(String.format("Total distinct values for partition column: %s",
                                        dbPartitionColumnValuesCount.size()));
                                log.info(dbPartitionColumnValuesCount.keySet().toString());
                                List<List<String>> partitionColumnBoundaries = new LinkedList<>();
                                int rc = 0;
                                List<String> partitionValues = new LinkedList<>();
                                for(String columnValue : dbPartitionColumnValuesCount.keySet()) {
                                    if((rc + dbPartitionColumnValuesCount.get(columnValue)) > maxPartitionSize) {
                                        partitionColumnBoundaries.add(partitionValues);
                                        partitionValues = new LinkedList<>();
                                        rc = dbPartitionColumnValuesCount.get(columnValue);
                                        partitionValues.add(columnValue);
                                    }
                                    else {
                                        partitionValues.add(columnValue);
                                        rc += dbPartitionColumnValuesCount.get(columnValue);
                                    }
                                }
                                if(partitionValues.size()>0) {
                                    partitionColumnBoundaries.add(partitionValues);
                                }

                                if(partitionColumnBoundaries.size()>20) {
                                    log.info("More than 20 paritions found. Consolidating all partitions into 20 since" +
                                            "BigQuery doesn't support more than 20 parallel writes");
                                    List<String> partitionValuesTemp = new LinkedList<>();
                                    for(int i=partitionColumnBoundaries.size()-1 ; i>=20; i--) {
                                        partitionValuesTemp = partitionColumnBoundaries.get(i);
                                        partitionColumnBoundaries.get(i%20).addAll(partitionValuesTemp);
                                        partitionColumnBoundaries.remove(i);
                                    }
                                }
                                
                                log.info(String.format("Num of partitions: %s", partitionColumnBoundaries.size()));
                                log.info(partitionColumnBoundaries.toString());

                                for(int i=0; i<partitionColumnBoundaries.size(); i++) {
                                    if(incrementalFilter.length()>1) {
                                        finalQuery = baseQuery + String.format("WHERE (%s AND ", incrementalFilter) +
                                                String.format(" %s IN (%s))", dbPartitionColumn,
                                                        StringUtils.join(partitionColumnBoundaries.get(i).stream().map(value -> "'" + value + "'").toArray(), ",")) +
                                                (i==0 ? String.format(" OR %s IS NULL", dbPartitionColumn) : "");
                                    }
                                    else {
                                        finalQuery = baseQuery + " WHERE " +
                                                String.format(" %s IN (%s)", dbPartitionColumn,
                                                        StringUtils.join(partitionColumnBoundaries.get(i).stream().map(value -> "'" + value + "'").toArray(), ",")) +
                                                (i==0 ? String.format(" OR %s IS NULL", dbPartitionColumn) : "");
                                    }
                                    log.info(String.format("Final Query %s : %s", i, finalQuery));
                                    p.apply(String.format("Read from DB part-%s", i+1), JdbcIO.<TableRow>read().withDataSourceConfiguration(
                                                            JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl).withUsername(user).withPassword(password))
                                                    .withQuery(finalQuery)
                                                    .withRowMapper(new MapToTableRow(dbSchema, bqSchema, dbType, replaceSpecialChars)))
                                            .apply(String.format("Write to BigQuery part-%s", i+1), BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", project, bqDataset, bqTable))
                                                    .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
                                }
                            }
                            else {
                                throw new Exception(String.format("isPartitionColValid is false. The given partition column %s is not a valid data type." +
                                        "Partition column should be either numeric or date or datetime type", dbPartitionColumn));
                            }
                        }
                        else{
                            throw new Exception(String.format("columnsMetadataResultset.next() is false"));
                        }
                    }

                }
                else{
                    throw new Exception(String.format("The row count is greater than threshold. Hence the query needs to be partitioned but dbPartitionColumn is null"));
                }
            }
            else {
                log.info("NO QUERY PARTITIONING");
                finalQuery = baseQuery + (incrementalFilter.length()>1 ? String.format(" WHERE %s", incrementalFilter) : "");
                log.info(String.format("Final query: %s", finalQuery));
                p.apply("Read from DB", JdbcIO.<TableRow>read().withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl).withUsername(user).withPassword(password))
                        .withQuery(finalQuery)
                        .withRowMapper(new MapToTableRow(dbSchema, bqSchema, dbType, replaceSpecialChars)))
                        .apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", project, bqDataset, bqTable))
                                .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
            }
        }
        p.run();

    }

    public static boolean validatePartitionColumn(String colDataType) {
        return numericTypes.contains(colDataType) || datetimeTypes.contains(colDataType)
                || dateTypes.contains(colDataType)
                || stringTypes.contains(colDataType);
    }

    public static List<List<String>> generatePartitionBoundaries(String lowerBound, String upperBound, String dataType, int numPartitions) {
        List<List<String>> intervals = new ArrayList<>();
        if(numericTypes.contains(dataType.toLowerCase())) {
            Double lb = Double.parseDouble(lowerBound);
            Double ub = Double.parseDouble(upperBound);
            Double step = Math.floor((ub-lb)/numPartitions);
            int x = 1;
            while(x<=numPartitions && lb+step<=ub) {
                intervals.add(Arrays.asList(String.valueOf(lb), String.valueOf(lb+step)));
                lb = lb + step;
                x += 1;
            }
            intervals.add(Arrays.asList(String.valueOf(lb), String.valueOf(ub)));
        }
        else if(dateTypes.contains(dataType.toLowerCase())) {
            LocalDate lbDate = convertStringToDate(lowerBound, Arrays.asList("yyyy-MM-dd"));
            LocalDate ubDate = convertStringToDate(upperBound, Arrays.asList("yyyy-MM-dd"));
            if(lbDate==null || ubDate==null) {
                return intervals;
            }
            long numOfDays = ChronoUnit.DAYS.between(lbDate, ubDate);
            int numOfDaysPerPartition = (int) (numOfDays/numPartitions);
            int x=1;
            while(x<=numPartitions && lbDate.plusDays(numOfDaysPerPartition).isBefore(ubDate)) {
                intervals.add(Arrays.asList(lbDate.toString(), lbDate.plusDays(numOfDaysPerPartition).toString()));
                lbDate = lbDate.plusDays(numOfDaysPerPartition);
                x += 1;
            }
            intervals.add(Arrays.asList(lbDate.toString(), ubDate.toString()));
        }
        else if(datetimeTypes.contains(dataType.toLowerCase())) {
            List<String> datetimeFormats = Arrays.asList("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss",
                    "yyyy-MM-dd HH:mm:ss.S","yyyy-MM-dd HH:mm:ss.n","yyyy-MM-dd'T'HH:mm:ss.S","yyyy-MM-dd'T'HH:mm:ss.n");
            LocalDateTime lbDateTime = convertStringToDateTime(lowerBound, datetimeFormats);
            LocalDateTime ubDateTime = convertStringToDateTime(upperBound, datetimeFormats);
            if(lbDateTime==null || ubDateTime==null) {
                return intervals;
            }
            long numOfDays = ChronoUnit.DAYS.between(lbDateTime.toLocalDate(), ubDateTime.toLocalDate());
            int numOfDaysPerPartition = (int) (numOfDays/numPartitions);
            int x=1;
            while(x<=numPartitions && lbDateTime.toLocalDate().plusDays(numOfDaysPerPartition).isBefore(ubDateTime.toLocalDate())) {
                intervals.add(Arrays.asList(lbDateTime.toString(), lbDateTime.toLocalDate().plusDays(numOfDaysPerPartition).toString()+"T00:00:00"));
                lbDateTime = lbDateTime.plusDays(numOfDaysPerPartition);
                x += 1;
            }
            intervals.add(Arrays.asList(lbDateTime.toLocalDate().plusDays(numOfDaysPerPartition).toString()+"T00:00:00", ubDateTime.toString()));
        }
        else if(stringTypes.contains(dataType.toLowerCase())) {

        }
        return intervals;
    }

    public static LocalDateTime convertStringToDateTime(String dateTimeString, List<String> dateTimeFormats) {
        DateTimeFormatter formatter = null;
        LocalDateTime dateTime = null;
        log.info(String.format("Evaluating %s with following date formats %s", dateTimeString, dateTimeFormats));
        for(String format : dateTimeFormats) {
            try {
                formatter = DateTimeFormatter.ofPattern(format);
                dateTime = LocalDateTime.parse(dateTimeString, formatter);
                log.info(String.format("Successfully parsed %s with format %s", dateTimeString, format));
                break;
            }
            catch (Exception e) {
                log.warn(String.format("Couldn't parse %s with format %s", dateTimeString, format));
            }
        }
        if(dateTime==null) {
            log.error(String.format("Couldn't parse %s with any of the date formats %s", dateTimeString, dateTimeFormats));
        }
        return dateTime;
    }

    public static LocalDate convertStringToDate(String dateString, List<String> dateFormats) {
        DateTimeFormatter formatter = null;
        LocalDate date = null;
        log.info(String.format("Evaluating %s with following date formats %s", dateString, dateFormats));
        for(String format : dateFormats) {
            try {
                formatter = DateTimeFormatter.ofPattern(format);
                date = LocalDate.parse(dateString, formatter);
                log.info(String.format("Successfully parsed %s with format %s", dateString, format));
                break;
            }
            catch (Exception e) {
                log.warn(String.format("Couldn't parse %s with format %s", dateString, format));
            }
        }
        if(date==null) {
            log.error(String.format("Couldn't parse %s with any of the date formats %s", dateString, dateFormats));
        }
        return date;
    }

    public static String getBaseQuery(String tableId, String dbType, String dbIncrementalColumn, String auditColumn, boolean replaceTable) {
        String query1 = "SELECT * FROM %s";
        String query2 = "SELECT *, %s AS %s FROM %s";
        String query3 = "SELECT *, CAST(%s AS %s) AS %s FROM %s";
        String baseQuery = null;
        log.info(String.format("dbIncrementalColumn: %s", dbIncrementalColumn));
        if(replaceTable) {
            baseQuery = String.format(query1, tableId);
        }
        else {
            if(auditColumn!=null && dbIncrementalColumn!=null && dbIncrementalColumn.equalsIgnoreCase(auditColumn)) {
                baseQuery = String.format(query1, tableId);
            }
            else if(auditColumn!=null && dbIncrementalColumn!=null && !dbIncrementalColumn.equalsIgnoreCase(auditColumn) ) {
                if(dbType.equalsIgnoreCase("postgresql")) {
                    baseQuery = String.format(query3, dbIncrementalColumn, "TIMESTAMP WITHOUT TIME ZONE", auditColumn, tableId);
                }
                else if(dbType.equalsIgnoreCase("mssql") || dbType.equalsIgnoreCase("sqlserver")
                        || dbType.equalsIgnoreCase("sql server") || dbType.equalsIgnoreCase("microsoft sql server")) {
                    baseQuery = String.format(query3, dbIncrementalColumn, "DATETIME", auditColumn, tableId);
                }
                else if(dbType.equalsIgnoreCase("mysql")) {
                    baseQuery = String.format(query3, dbIncrementalColumn, "DATETIME", auditColumn, tableId);
                }
                else if(dbType.equalsIgnoreCase("oracle")) {
                    baseQuery = String.format(query3, dbIncrementalColumn, "TIMESTAMP", auditColumn, tableId);
                }
            }
            else {
                if(dbType.equalsIgnoreCase("postgresql")) {
                    baseQuery = String.format(query2, "NOW()", auditColumn, tableId);
                }
                else if(dbType.equalsIgnoreCase("mssql") || dbType.equalsIgnoreCase("sqlserver")
                        || dbType.equalsIgnoreCase("sql server") || dbType.equalsIgnoreCase("microsoft sql server")) {
                    baseQuery = String.format(query2, "GETUTCDATE()", auditColumn, tableId);
                }
                else if(dbType.equalsIgnoreCase("mysql")) {
                    baseQuery = String.format(query2, "UTC_TIMESTAMP()", auditColumn, tableId);
                }
                else if(dbType.equalsIgnoreCase("oracle")) {
                    baseQuery = String.format(query2, "SYS_EXTRACT_UTC(CURRENT_TIMESTAMP)", auditColumn, tableId);
                }
            }
        }
        return baseQuery;
    }


}
