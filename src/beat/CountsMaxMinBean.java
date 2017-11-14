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
public class CountsMaxMinBean {

    private final static Logger LOGGER = LoggerFactory.getLogger(CountsMaxMinBean.class);
    public SimpleStringProperty srcCol = new SimpleStringProperty();
    public SimpleStringProperty srcColCount = new SimpleStringProperty();
    public SimpleStringProperty trgCol = new SimpleStringProperty();
    public SimpleStringProperty trgColCount = new SimpleStringProperty();
    public SimpleStringProperty result = new SimpleStringProperty();

    public String getSrcCol() {
        return srcCol.get();
    }

    public String getSrcColCount() {
        return srcColCount.get();
    }

    public String getTrgCol() {
        return trgCol.get();
    }

    public String getTrgColCount() {
        return trgColCount.get();
    }

    public String getResult() {
        return result.get();
    }

    @Override
    public String toString() {
        LOGGER.info("Retreived Count/ Max/ Min data");
        return srcCol.get() + "," + srcColCount.get() + "," + trgCol.get() + "," + trgColCount.get() + "," + result.get();
    }

}
