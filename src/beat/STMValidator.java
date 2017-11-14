/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. **/

package beat;

import java.io.File;

/**
 *
 * @author Ravindra
 */
public class STMValidator {

    public boolean checkSrcTrgFiles(String srcfile, String trgfile) {

        System.out.println("checkSrcTrgFiles - called");
        System.out.println("[INFO] SRC FILE NAME : " + srcfile + " TRG FILE NAME : " + trgfile);

        File src = new File(srcfile.trim());
        File trg = new File(trgfile.trim());

        if (src.exists() && trg.exists() && src.isFile() && trg.isFile()) {

            return true;
        } else {
            return false;
        }
    }

}
