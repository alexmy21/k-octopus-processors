/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.processors.util;

/**
 *
 * @author alexmy
 */
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public class Json2Xml {
    
    static final Logger LOG = Logger.getLogger(Json2Xml.class.getName());

    public static void main(String... s) {
        try {
            String json_data = "{\"student\":{\"name\":\"Neeraj Mishra\", \"age\":\"22\"}}";
            JSONObject obj = new JSONObject(json_data);

            //converting json to xml
            String xml_data = XML.toString(obj);

            System.out.println(xml_data);
        } catch (JSONException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }
    }
}
