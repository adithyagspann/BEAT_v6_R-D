/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class ClassLoaderEngine {

    String jarpath = "";
    private Logger logger = LoggerFactory.getLogger(ClassLoaderEngine.class);

    public ClassLoaderEngine(String jarpath) {
        this.jarpath = jarpath;
        loadClasses(jarpath);
    }

    public void loadClasses(String dirpath) {
        logger.info("Loading the jarpath :" + dirpath);

        try {
            System.out.println("Dirpath: " + dirpath);
            File directory = new File(dirpath);
            File[] fList = directory.listFiles();

            for (File file : fList) {
                if (file.isFile()) {
                    // System.out.println(file.getAbsolutePath());
                    URL url = file.toURI().toURL();
                    URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                    method.setAccessible(true);
                    method.invoke(classLoader, url);

                } else if (file.isDirectory()) {
                    loadClasses(file.getAbsolutePath());
                }
            }
        } catch (MalformedURLException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            logger.info("Execption Raised while loading the jarpath :" + ex.toString());
            new ExceptionUI(ex);
        }
    }

}
