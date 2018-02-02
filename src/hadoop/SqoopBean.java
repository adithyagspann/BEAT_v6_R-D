package hadoop;

import javafx.beans.property.SimpleStringProperty;

/**
 *
 * @author Admin
 * @description Sqoop Bean for Sqoop Query Generator
 */
public class SqoopBean {

    private SimpleStringProperty sqopJdbcUrl;
    private SimpleStringProperty sqopDriverclass;
    private SimpleStringProperty sqopUserName;
    private SimpleStringProperty sqoopPasswd;
    private SimpleStringProperty sqopSrcSchema;
    private SimpleStringProperty sqopSrcTbl;
    private SimpleStringProperty sqopSrcWhere;
    private SimpleStringProperty sqopTrgSchema;
    private SimpleStringProperty sqopTrgTable;
    private SimpleStringProperty sqopTrgDir;
    private SimpleStringProperty sqopFieldDelimt = new SimpleStringProperty("\t");
    private SimpleStringProperty sqopLineDelimt = new SimpleStringProperty("\n");
    private SimpleStringProperty sqopMapper;
    private SimpleStringProperty sqopSplitBy;

    public String getSqopJdbcUrl() {
        return sqopJdbcUrl.getValue();
    }

    public void setSqopJdbcUrl(SimpleStringProperty sqopJdbcUrl) {
        this.sqopJdbcUrl = sqopJdbcUrl;
    }

    public String getSqopDriverclass() {
        return sqopDriverclass.getValue();
    }

    public void setSqopDriverclass(SimpleStringProperty sqopDriverclass) {
        this.sqopDriverclass = sqopDriverclass;
    }

    public String getSqopUserName() {
        return sqopUserName.getValue();
    }

    public void setSqopUserName(SimpleStringProperty sqopUserName) {
        this.sqopUserName = sqopUserName;
    }

    public String getSqoopPasswd() {
        return sqoopPasswd.getValue();
    }

    public void setSqoopPasswd(SimpleStringProperty sqoopPasswd) {
        this.sqoopPasswd = sqoopPasswd;
    }

    public String getSqopSrcSchema() {
        return sqopSrcSchema.getValue();
    }

    public void setSqopSrcSchema(SimpleStringProperty sqopSrcSchema) {
        this.sqopSrcSchema = sqopSrcSchema;
    }

    public String getSqopSrcTbl() {
        return sqopSrcTbl.getValue();
    }

    public void setSqopSrcTbl(SimpleStringProperty sqopSrcTbl) {
        this.sqopSrcTbl = sqopSrcTbl;
    }

    public String getSqopSrcWhere() {
        return sqopSrcWhere.getValue();
    }

    public void setSqopSrcWhere(SimpleStringProperty sqopSrcWhere) {
        this.sqopSrcWhere = sqopSrcWhere;
    }

    public String getSqopTrgSchema() {
        return sqopTrgSchema.getValue();
    }

    public void setSqopTrgSchema(SimpleStringProperty sqopTrgSchema) {
        this.sqopTrgSchema = sqopTrgSchema;
    }

    public String getSqopTrgTable() {
        return sqopTrgTable.getValue();
    }

    public void setSqopTrgTable(SimpleStringProperty sqopTrgTable) {
        this.sqopTrgTable = sqopTrgTable;
    }

    public String getSqopTrgDir() {
        return sqopTrgDir.getValue();
    }

    public void setSqopTrgDir(SimpleStringProperty sqopTrgDir) {
        this.sqopTrgDir = sqopTrgDir;
    }

    public String getSqopFieldDelimt() {
        return sqopFieldDelimt.getValue();
    }

    public void setSqopFieldDelimt(SimpleStringProperty sqopFieldDelimt) {
        this.sqopFieldDelimt = sqopFieldDelimt;
    }

    public String getSqopLineDelimt() {
        return sqopLineDelimt.getValue();
    }

    public void setSqopLineDelimt(SimpleStringProperty sqopLineDelimt) {
        this.sqopLineDelimt = sqopLineDelimt;
    }

    public String getSqopMapper() {
        return sqopMapper.getValue();
    }

    public void setSqopMapper(SimpleStringProperty sqopMapper) {
        this.sqopMapper = sqopMapper;
    }

    public String getSqopSplitBy() {
        return sqopSplitBy.getValue();
    }

    public void setSqopSplitBy(SimpleStringProperty sqopSplitBy) {
        this.sqopSplitBy = sqopSplitBy;
    }

}