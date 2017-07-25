package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesLoader {


    public static Properties load(String filename){
        InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(filename);
        Properties props = new Properties();
        try {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static void show(String propertyFile) throws IOException {

        Properties props = PropertiesLoader.load(propertyFile);
        Enumeration<Object> keys = props.keys();

        while(keys.hasMoreElements()){
            Object key = keys.nextElement();
            System.out.println(key + ": " + props.get(key));
        }
    }

    public static void main(String[] args) throws IOException {

        show("kafka.properties");





    }

}
