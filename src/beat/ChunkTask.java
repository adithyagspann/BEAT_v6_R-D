/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javafx.collections.ObservableList;
import org.apache.commons.collections4.CollectionUtils;

/**
 *
 * @author Snehasish
 */
public class ChunkTask implements Callable<List>{
   
       List source;
       List target;
       ChunkTask(List operand1,List operand2)
       {
             this.source = operand1;
             this.target = operand2;             
       }          
       @Override
       public List call() throws Exception {
              List srcremovedList = (ArrayList) CollectionUtils.removeAll(source, target);
              return srcremovedList;
       } 
}
