package hadoop;

import java.io.File;

/**
 *
 * @author Adithya
 */
public class HadoopCommonFunction {

    /**
     * Runs the sqoop import and returns status
     *
     * @param srcJdbcUrl         jdbc url for source database; argument is
     *                           mandatory
     * @param srcDBUserName      user name for source database; argument is
     *                           mandatory
     * @param srcDBPassword      password for source database
     * @param srcSchemaName      schema name for source database; argument is
     *                           mandatory
     * @param srcTableName       table name for source; argument is mandatory
     * @param srcColumns         columns to be extracted from source table;
     *                           argument may be null, in which case all the
     *                           columns are extracted from source table
     * @param srcWhereCondn      extract criteria;argument may be null, in which
     *                           case entire source table data is extracted
     * @param tgtSchemaName      scheme name for target database; argument is
     *                           mandatory
     * @param tgtTableName       table name for target; argument is mandatory
     * @param tmpPath            temporary path for sqoop process
     * @param tmpFile            temporary file name for sqoop process
     * @param numMapper          number of mappers for the sqoop import;
     *                           argument may be null, in which case default
     *                           value of 1 is assumed
     * @param fieldTerminator    field terminator; argument may be null, in
     *                           which case default value of """^""" is assumed
     * @param lineTerminator     line terminator; argument may be null, in which
     *                           case default value of "\n" is assumed
     * @param tgtTableCreateInd  specify true if target table needs to be
     *                           created; argument may be null, in which case
     *                           default value of false is assumed and and table
     *                           is not created
     * @param partitionKey       partition key column for the target table;
     *                           argument may be null, in which case default
     *                           value of 1
     * @param partitionValue     partition value for the target table
     * @param tgtOverwriteInd    specify true if target table data has to be
     *                           overwritten; argument may be null, in which
     *                           case default value of false is assumed and data
     *                           will be appended
     * @param hiveDropDelimiters specify true if the hive drop delimiters option
     *                           is required; default is false
     * @param dependentJars      name of the dependent jar files - specify
     *                           absolute file path
     * @param jdbcDriver         jdbc driver class that has to be used by sqoop;
     *                           argument may be null, in which case default
     *                           driver will be used
     * @param splitByColumn      Column on which the data has to be split while
     *                           reading from source
     * @param srcDBPasswordFile  password file location; if both
     *                           srcDBPasswordFile and srcDBPassword are passed,
     *                           password file will be given precedence
     * @param hiveMapCol         this option is to map any specific column in
     *                           the hive with give datatype
     * @param outputSrcFileDir
     * @param outputBinDir
     * @param app_nm
     * @param prcs_nm
     * @param task_seq_id
     * @param charConvDecdr
     * @return true if the sqoop import is successful else false
     */
    public void sqoopDataImport(String srcJdbcUrl,
            String srcDBUserName,
            String srcDBPassword,
            String hiveMapCol,
            String srcSchemaName,
            String srcTableName,
            String srcColumns,
            String srcWhereCondn,
            String tgtSchemaName,
            String tgtTableName,
            String tmpPath,
            String numMapper,
            String fieldTerminator,
            String lineTerminator,
            boolean tgtTableCreateInd,
            String partitionKey,
            String partitionValue,
            boolean tgtOverwriteInd,
            boolean hiveDropDelimiters,
            String dependentJars,
            String jdbcDriver,
            String splitByColumn,
            String srcDBPasswordFile,
            String outputSrcFileDir,
            boolean charConvDecdr) {

        //audit entry
        int invalidParam = 0;

        // Validate parameters
        if (srcJdbcUrl == "" || srcDBUserName == "" || srcTableName == "" || tgtSchemaName == "" || tgtTableName == "" || tmpPath == "") {
            invalidParam = 1;
        } else if ((partitionKey != "" && partitionValue == "") || (partitionKey == "" && partitionValue != "")) {
            invalidParam = 1;
        }

//        val currTs = getTodayFormatted("YYYYMMddhhmmss")
//        val optFileName = s
//        "sqoopoptions_${tgtTableName}_$currTs.txt"
//
//    val optFile
//        :File = new File(optFileName)
        StringBuilder sqoopImpQuery = null;
        if (invalidParam == 1) {
            //throw new IllegalArgumentException("Invalid Parameters")
            System.out.println("[ERROR] One or more mandatory parameters missing. Please verify");

        } else {
            String hiveOverwrite = tgtOverwriteInd ? "--hive-overwrite" : "";

            String hiveDropdelims = hiveDropDelimiters ? "--hive-drop-import-delims" : "";
            String whereClause = !"".equals(srcWhereCondn) ? "--where" : "";
            String hiveCreateTable = (tgtTableCreateInd) ? "--create-hive-table" : "";

            String partitionKeyClause = (!"".equals(partitionKey) && partitionKey.length() > 0) ? "--hive-partition-key" : "";

            String partitionValClause = (!"".equals(partitionValue) && partitionValue.length() > 0) ? "--hive-partition-value" : "";
            String srcColClause = (!"".equals(srcColumns)) ? "--columns" : "";
            String libJar = !"".equals(dependentJars) ? "-libjars" : "";

            String driverParam = !"".equals(jdbcDriver) ? "--driver" : "";

            String spltBy = (!"".equals(splitByColumn)) ? "--split-by" : "";
            String fieldTerminatorModified = fieldTerminator;
            String lineTerminatorModified = lineTerminator;
            String nullValue = "\\N";

            String srcPwdOption = srcDBPasswordFile.isEmpty() ? "--password" : "--password-file";

            String srcPwd = srcDBPasswordFile.isEmpty() ? srcDBPassword : srcDBPasswordFile;

            String srcHiveMapCol = !"".equals(hiveMapCol) ? "--map-column-hive" : "";

            String charConvDecodProp = charConvDecdr ? "-Dmapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Ddb2.jcc.charsetDecoderEncoder=3" : "";

            sqoopImpQuery = new StringBuilder("sqoop import");
            sqoopImpQuery.append(charConvDecodProp).append(" ").append(libJar).append(" ").append(dependentJars).append(" --connect ").append(srcJdbcUrl).append(" ").append(driverParam);
            sqoopImpQuery.append(" ").append(jdbcDriver).append(" --username ").append(srcDBUserName).append(" ").append(srcPwdOption).append(" ").append(srcPwd);
            sqoopImpQuery.append(" ").append(srcHiveMapCol).append(" ").append(hiveMapCol).append(" --table ").append(srcTableName).append(" ").append(whereClause).append(" ").append(srcWhereCondn);
            sqoopImpQuery.append(" ").append(srcColClause).append(" ").append(srcColumns).append(" ").append(spltBy).append(" ").append(splitByColumn).append(" --as-textfile --fields-terminated-by ").append(fieldTerminatorModified);
            sqoopImpQuery.append(" --lines-terminated-by " + lineTerminatorModified + " --hive-import " + hiveCreateTable);
            sqoopImpQuery.append(" ").append(hiveOverwrite).append(" ").append(hiveDropdelims).append(" --hive-table ").append(tgtSchemaName).append(".").append(tgtTableName);
            sqoopImpQuery.append(" ").append(partitionKeyClause).append(" ").append(partitionKey).append(" ").append(partitionValClause).append(" ").append(partitionValue);
            sqoopImpQuery.append(" --delete-target-dir --null-string ").append(nullValue).append(" --null-not-string ").append(nullValue);
            sqoopImpQuery.append(" --targer-dir ").append(tmpPath).append(" -m ").append((numMapper.isEmpty() || numMapper.equals("0") ? "1" :  numMapper)).append(" --schema ").append(srcSchemaName).append(" --outdir ").append(outputSrcFileDir);

        }

    }

    /**
     * Runs the sqoop import and returns status
     *
     * @param srcJdbcUrl         jdbc url for source database; argument is
     *                           mandatory
     * @param srcDBUserName      user name for source database; argument is
     *                           mandatory
     * @param srcDBPassword      password for source database
     * @param srcQuery           query to extract data from source; argument is
     *                           mandatory
     * @param tgtSchemaName      scheme name for target database; argument is
     *                           mandatory
     * @param tgtTableName       table name for target; argument is mandatory
     * @param tmpPath            temporary path for sqoop process
     * @param tmpFile            temporary file name for sqoop process
     * @param numMapper          number of mappers for the sqoop import;
     *                           argument may be null, in which case default
     *                           value of 1 is assumed
     * @param fieldTerminator    field terminator; argument may be null, in
     *                           which case default value of """^""" is assumed
     * @param lineTerminator     line terminator; argument may be null, in which
     *                           case default value of "\n" is assumed
     * @param tgtTableCreateInd  specify true if target table needs to be
     *                           created; argument may be null, in which case
     *                           default value of false is assumed and and table
     *                           is not created
     * @param partitionKey       partition key column for the target table;
     *                           argument may be null, in which case default
     *                           value of 1
     * @param partitionValue     partition value for the target table
     * @param tgtOverwriteInd    specify true if target table data has to be
     *                           overwritten; argument may be null, in which
     *                           case default value of false is assumed and data
     *                           will be appended
     * @param hiveDropDelimiters specify true if the hive drop delimiters option
     *                           is required; default is false
     * @param dependentJars      name of the dependent jar files - specify
     *                           absolute file path
     * @param jdbcDriver         jdbc driver class that has to be used by sqoop;
     *                           argument may be null, in which case default
     *                           driver will be used
     * @param splitByColumn      Column on which the data has to be split while
     *                           reading from source
     * @param srcDBPasswordFile  password file location; if both
     *                           srcDBPasswordFile and srcDBPassword are passed,
     *                           password file will be given precedence
     * @param hiveMapCol         this option is to map any specific column in
     *                           the hive with give datatype
     * @param outputSrcFileDir
     * @param outputBinDir
     * @param app_nm
     * @param prcs_nm
     * @param task_seq_id
     * @return true if the sqoop import is successful else false
     */
    public String sqoopDataImportQuery(String srcJdbcUrl,
            String srcDBUserName,
            String srcDBPassword,
            String hiveMapCol,
            String srcQuery,
            String tgtSchemaName,
            String tgtTableName,
            String tmpPath,
            String tmpFile,
            String numMapper,
            String fieldTerminator,
            String lineTerminator,
            Boolean tgtTableCreateInd,
            String partitionKey,
            String partitionValue,
            Boolean tgtOverwriteInd,
            boolean hiveDropDelimiters,
            String dependentJars,
            String jdbcDriver,
            String splitByColumn,
            String srcDBPasswordFile,
            String outputSrcFileDir,
            String app_nm,
            String prcs_nm,
            String task_seq_id) {

        //audit entry
        int invalidParam = 0;

        // Validate parameters
        if (srcJdbcUrl.trim() == "" || srcDBUserName.trim() == "" || srcQuery.trim() == "" || tgtSchemaName.trim() == ""
                || tgtTableName.trim() == "" || tmpPath.trim() == "" || tmpFile.trim() == "") {
            invalidParam = 1;
        } else if ((partitionKey.trim() != "" && partitionValue.trim() == "") || (partitionKey.trim() == "" && partitionValue.trim() != "")) {
            invalidParam = 1;
        }

//        String currTs = getTodayFormatted("yyyyMMddhhmmss");
        String optFileName = "sqoopoptions_${tgtTableName}_$currTs.txt";
        File optFile = new File(optFileName);
        StringBuilder sqoopCmdQry = new StringBuilder();
        if (invalidParam == 1) {
            //throw new IllegalArgumentException("Invalid Paramaters")
            System.out.println("[ERROR] One or more mandatory parameters missing. Please verify");
            //writeToLog("sqoopDataImportQuery","INFO",s"One or more mandatory parameters missing. Please verify")

        } else {
            String hiveOverwrite = tgtOverwriteInd ? "--hive-overwrite" : "";
            String hiveDropdelims = hiveDropDelimiters ? "--hive-drop-import-delims" : "";
            String hiveCreateTable = tgtTableCreateInd ? "--create-hive-table" : "";
            String partitionKeyClause = (partitionKey != "" && partitionKey.length() > 0) ? "--hive-partition-key" : "";
            String partitionValClause = (partitionValue != "" && partitionValue.length() > 0) ? "--hive-partition-value" : "";
            String libJar = dependentJars != "" ? "-libjars" : "";
            String driverParam = jdbcDriver != "" ? "--driver" : "";
            String spltBy = splitByColumn != "" ? "--split-by" : "";
            String fieldTerminatorModified = "$fieldTerminator";
            String lineTerminatorModified = lineTerminator;
            String nullValue = "\\N";
            String srcPwdOption = srcDBPasswordFile.isEmpty() ? "--password" : "--password-file";
            String srcPwd = srcDBPasswordFile.isEmpty() ? srcDBPassword : srcDBPasswordFile;
            String srcHiveMapCol = hiveMapCol != "" ? "--map-column-hive" : "";

            sqoopCmdQry.append("sqoop import");
            sqoopCmdQry.append(libJar + " " + dependentJars);
            sqoopCmdQry.append(" --connect " + srcJdbcUrl);
            sqoopCmdQry.append(" " + driverParam + " ");
            sqoopCmdQry.append(jdbcDriver);
            sqoopCmdQry.append(" --username " + srcDBUserName);
            sqoopCmdQry.append(srcPwdOption + " " + srcPwd);
            sqoopCmdQry.append(" " + srcHiveMapCol + " " + hiveMapCol + " ");
            sqoopCmdQry.append(" --query \"" + srcQuery + "\" ");
            sqoopCmdQry.append(spltBy + " " + splitByColumn);
            sqoopCmdQry.append(" --as-textfile --fields-terminated-by " + fieldTerminatorModified);
            sqoopCmdQry.append(" --lines-terminated-by " + lineTerminatorModified);
            sqoopCmdQry.append(" --hive-import " + hiveCreateTable + " " + hiveOverwrite + " ");
            sqoopCmdQry.append(hiveDropdelims + " --hive-table ");
            sqoopCmdQry.append(tgtSchemaName + "." + tgtTableName);
            sqoopCmdQry.append(" " + partitionKeyClause + " ");
            sqoopCmdQry.append(partitionKey + " " + partitionValClause + " " + partitionValue + " --delete-target-dir ");
            sqoopCmdQry.append(" --null-String " + nullValue + " ");
            sqoopCmdQry.append(" --null-non-String " + nullValue);
            sqoopCmdQry.append(" --target-dir " + tmpPath + "/" + tmpFile);
            sqoopCmdQry.append(" -m " + numMapper);
            sqoopCmdQry.append(" --outdir " + outputSrcFileDir);

            String cmdString = "";

            System.out.println("[INFO] Sqoop command output status : $isSqoopSuccess");

        }

        return sqoopCmdQry.toString();

    }

}
