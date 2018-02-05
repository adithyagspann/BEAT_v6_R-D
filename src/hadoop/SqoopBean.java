package hadoop;

import java.io.File;
import java.util.Map;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;

/**
 *
 * @author Admin
 * @description Sqoop Bean for Sqoop Query Generator
 */
public class SqoopBean {

    private SimpleStringProperty sqopDBType = new SimpleStringProperty("");
    private SimpleStringProperty sqopJdbcUrl = new SimpleStringProperty("");
    private SimpleStringProperty sqopDriverclass = new SimpleStringProperty("");
    private SimpleStringProperty sqopUserName = new SimpleStringProperty("");
    private SimpleStringProperty sqoopPasswd = new SimpleStringProperty("");
    private SimpleStringProperty sqopSrcSchema = new SimpleStringProperty("");
    private SimpleStringProperty sqopSrcTbl = new SimpleStringProperty("");
    private SimpleStringProperty sqopSrcWhere = new SimpleStringProperty("");
    private SimpleStringProperty sqopTrgSchema = new SimpleStringProperty("");
    private SimpleStringProperty sqopTrgTable = new SimpleStringProperty("");
    private SimpleStringProperty sqopTrgDir = new SimpleStringProperty("");
    private SimpleStringProperty sqopFieldDelimt = new SimpleStringProperty("\t");
    private SimpleStringProperty sqopLineDelimt = new SimpleStringProperty("\n");
    private SimpleStringProperty sqopMapper = new SimpleStringProperty("");
    private SimpleStringProperty sqopSplitBy = new SimpleStringProperty("");
    private SimpleStringProperty sqopColmns = new SimpleStringProperty("");
    private SimpleStringProperty sqopTgtExist = new SimpleStringProperty("");
    private SimpleStringProperty sqopSeverName = new SimpleStringProperty("");
    private SimpleStringProperty sqopServerusrnme = new SimpleStringProperty("");
    private SimpleStringProperty sqopServerpswd = new SimpleStringProperty("");
    private Map<File,String> filePath = FXCollections.observableHashMap();
    private String sqoopServerPath = "";

    public String getSqoopServerPath() {
        return sqoopServerPath;
    }

    public void setSqoopServerPath(String sqoopServerPath) {
        this.sqoopServerPath = sqoopServerPath;
    }

    public Map<File,String> getFilePath() {
        return filePath;
    }

    public void setFilePath(Map filePath) {
        this.filePath = filePath;
    }

    public String getSqopSeverName() {
        return sqopSeverName.getValue();
    }

    public void setSqopSeverName(String sqopSeverName) {
        this.sqopSeverName.setValue(sqopSeverName);
    }

    public String getSqopServerusrnme() {
        return sqopServerusrnme.getValue();
    }

    public void setSqopServerusrnme(String sqopServerusrnme) {
        this.sqopServerusrnme.setValue(sqopServerusrnme);
    }

    public String getSqopServerpswd() {
        return sqopServerpswd.getValue();
    }

    public void setSqopServerpswd(String sqopServerpswd) {
        this.sqopServerpswd.setValue(sqopServerpswd);
    }

    public String getSqopTgtExist() {
        return sqopTgtExist.getValue();
    }

    public void setSqopTgtExist(String sqopTgtExist) {
        this.sqopTgtExist.setValue(sqopTgtExist);
    }

    public String getSqopColmns() {
        return sqopColmns.getValue();
    }

    public void setSqopColmns(String sqopColmns) {
        this.sqopColmns.set(sqopColmns);
    }

    public String getSqopDBType() {
        return sqopDBType.getValue();
    }

    public void setSqopDBType(String sqopDBType) {
        this.sqopDBType.set(sqopDBType);
    }

    public String getSqopJdbcUrl() {
        return sqopJdbcUrl.getValue();
    }

    public void setSqopJdbcUrl(String sqopJdbcUrl) {
        this.sqopJdbcUrl.set(sqopJdbcUrl);
    }

    public String getSqopDriverclass() {
        return sqopDriverclass.getValue();
    }

    public void setSqopDriverclass(String sqopDriverclass) {
        this.sqopDriverclass.set(sqopDriverclass);
    }

    public String getSqopUserName() {
        return sqopUserName.getValue();
    }

    public void setSqopUserName(String sqopUserName) {
        this.sqopUserName.set(sqopUserName);
    }

    public String getSqoopPasswd() {
        return sqoopPasswd.getValue();
    }

    public void setSqoopPasswd(String sqoopPasswd) {
        this.sqoopPasswd.set(sqoopPasswd);
    }

    public String getSqopSrcSchema() {
        return sqopSrcSchema.getValue();
    }

    public void setSqopSrcSchema(String sqopSrcSchema) {
        this.sqopSrcSchema.set(sqopSrcSchema);
    }

    public String getSqopSrcTbl() {
        return sqopSrcTbl.getValue();
    }

    public void setSqopSrcTbl(String sqopSrcTbl) {
        this.sqopSrcTbl.set(sqopSrcTbl);
    }

    public String getSqopSrcWhere() {
        return sqopSrcWhere.getValue();
    }

    public void setSqopSrcWhere(String sqopSrcWhere) {
        this.sqopSrcWhere.set(sqopSrcWhere);
    }

    public String getSqopTrgSchema() {
        return sqopTrgSchema.getValue();
    }

    public void setSqopTrgSchema(String sqopTrgSchema) {
        this.sqopTrgSchema.set(sqopTrgSchema);
    }

    public String getSqopTrgTable() {
        return sqopTrgTable.getValue();
    }

    public void setSqopTrgTable(String sqopTrgTable) {
        this.sqopTrgTable.set(sqopTrgTable);
    }

    public String getSqopTrgDir() {
        return sqopTrgDir.getValue();
    }

    public void setSqopTrgDir(String sqopTrgDir) {
        this.sqopTrgDir.set(sqopTrgDir);
    }

    public String getSqopFieldDelimt() {
        return sqopFieldDelimt.getValue();
    }

    public void setSqopFieldDelimt(String sqopFieldDelimt) {
        this.sqopFieldDelimt.set(sqopFieldDelimt);
    }

    public String getSqopLineDelimt() {
        return sqopLineDelimt.getValue();
    }

    public void setSqopLineDelimt(String sqopLineDelimt) {
        this.sqopLineDelimt.set(sqopLineDelimt);
    }

    public String getSqopMapper() {
        return sqopMapper.getValue();
    }

    public void setSqopMapper(String sqopMapper) {
        this.sqopMapper.set(sqopMapper);
    }

    public String getSqopSplitBy() {
        return sqopSplitBy.getValue();
    }

    public void setSqopSplitBy(String sqopSplitBy) {
        this.sqopSplitBy.set(sqopSplitBy);
    }

    @Override
    public String toString() {
        return "sqopDBType=" + sqopDBType.getValue() + ", sqopJdbcUrl=" + sqopJdbcUrl.getValue() + ", sqopDriverclass=" + sqopDriverclass.getValue() + ", sqopUserName=" + sqopUserName.getValue() + ", sqoopPasswd=" + sqoopPasswd.getValue() + ", sqopSrcSchema=" + sqopSrcSchema.getValue() + ", sqopSrcTbl=" + sqopSrcTbl.getValue() + ", sqopSrcWhere=" + sqopSrcWhere.getValue() + ", sqopTrgSchema=" + sqopTrgSchema.getValue() + ", sqopTrgTable=" + sqopTrgTable.getValue() + ", sqopTrgDir=" + sqopTrgDir.getValue() + ", sqopFieldDelimt=" + sqopFieldDelimt.getValue() + ", sqopLineDelimt=" + sqopLineDelimt.getValue() + ", sqopMapper=" + sqopMapper.getValue() + ", sqopSplitBy=" + sqopSplitBy.getValue() + ", sqopColms=" + sqopColmns.getValue();
    }

}
