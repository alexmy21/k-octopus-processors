/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.compute;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import java.util.Map;
import org.lisapark.koctopus.core.graph.GraphUtils;
import org.lisapark.koctopus.util.Pair;
import org.openide.util.Exceptions;

/**
 *
 * @author alexmy
 */
public class MainMultimap {

    public static void main(String[] args) {
        String str= "one:two";
        String[] stra = str.split(":");
        
        System.out.println(stra);
        
        
//        Integer intg = new Integer();
//        
//        String str = new String();
        try {
            Multimap<String, Pair<String, String>> map = HashMultimap.create();
            map.put("one", new Pair("name", "One"));
            map.put("one", new Pair("age", "72"));
            map.put("two", new Pair("name", "Two"));
            map.put("two", new Pair("age", "43"));
            map.put("two", new Pair("Address", "Russia"));
            
//            Gson gson = GraphUtils.gsonGnodeMeta(map);
//
//            String jsonString = gson.toJson(map);
//            System.out.println(jsonString);
//
//            Multimap<String, Pair<String, String>> map2 = gson.fromJson(jsonString, GraphUtils.HASH_MULTIMAP_PAIR);
//            System.out.println(new Gson().toJson(map2.asMap(), Map.class));
//
//            System.out.println(gson.toJson(map2));
        } catch (JsonParseException ex) {
            Exceptions.printStackTrace(ex);
        }
    }
}