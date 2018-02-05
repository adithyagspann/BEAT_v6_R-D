/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.* */
package beat;

import javafx.beans.property.SimpleStringProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adithya
 */

/*Bean for all other Counts on the STM data except Total Counts & Advanced table data Processing */
public class DataVdlSummry {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataVdlSummry.class);
    public SimpleStringProperty valditionType = new SimpleStringProperty();
    public SimpleStringProperty srcCnt = new SimpleStringProperty();
    public SimpleStringProperty trgCnt = new SimpleStringProperty();
    public SimpleStringProperty result = new SimpleStringProperty();

    public String getValditionType() {
        return valditionType.get();
    }

    public String getSrcCnt() {
        return srcCnt.get();
    }

    public String getTrgCnt() {
        return trgCnt.get();
    }

   
    public String getResult() {
        return result.get();
    }

    @Override
    public String toString() {
        LOGGER.info("Retreived Data Validation data");
        return valditionType.get() + "," + srcCnt.get() + "," + trgCnt.get() + "," + result.get();
    }

}
