package beat;

import javafx.beans.property.SimpleStringProperty;

public class FFColMetaData {

    private SimpleStringProperty columnName = new SimpleStringProperty();
    private SimpleStringProperty colType = new SimpleStringProperty();
    private SimpleStringProperty colSize = new SimpleStringProperty();

    private int begin, end;

    public int getBegin() {
        return begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public FFColMetaData(String columnName, String colType, String colSize) {
        this.columnName.setValue(columnName);

        this.colType.setValue(colType);
        this.colSize.setValue(colSize);
    }

    public String getColumnName() {
        return columnName.getValue();
    }

    public void setColumnName(String columnName) {
        this.columnName.setValue(columnName);
    }

    public String getColType() {
        return colType.getValue();
    }

    public void setColType(String colType) {
        this.colType.setValue(colType);
    }

    public String getColSize() {
        return colSize.getValue();
    }

    public void setColSize(String colSize) {
        this.colSize.setValue(colSize);
    }

    public String getFixedLenMeta() {

        return "'name'#'" + columnName.getValue() + "'\n'type'#'" + colType.getValue() + "'\n'begin'#'" + getBegin() + "'\n'end'#'" + getEnd() + "'";
    }

    @Override
    public String toString() {

        return "'name'#'" + columnName.getValue() + "'\n'type'#'" + colType.getValue() + "'";

    }

}
