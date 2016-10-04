/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tokenizer.controller;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author user
 */
public class AllFieldTokenizer {

    /**
     * author: Elisafina
     * menggabungkan hashmap yang didapat dari masing-masing field, sehingga didapat hashmap untuk 1 hashmap
     * yang nenampung seluruh isi field date, to, from, subject, body (untuk field "all")
     * @param date
     * @param to
     * @param from
     * @param subject
     * @param body
     * @return hashmap untuk all field
     */
    public static HashMap<String, Integer> allFieldTermList(HashMap<String, Integer> date, HashMap<String, Integer> to, HashMap<String, Integer> from, HashMap<String, Integer> subject, HashMap<String, Integer> body) {
        HashMap<String, Integer> all = new HashMap<String, Integer>();
        all = mergeHashMap(all, date);
        all = mergeHashMap(all, to);
        all = mergeHashMap(all, from);
        all = mergeHashMap(all, subject);
        all = mergeHashMap(all, body);
        return all;

    }

    
    /**
     * author: Elisafina
     * adalah method untuk menggabungkan suatu hashmap dengan hashmap lain. 
     * @param a hashmap pertama
     * @param b hashmap kedua
     * @return hasil penggabungan hashmap pertama dan kedua
     */
    public static HashMap<String, Integer> mergeHashMap(HashMap<String, Integer> a, HashMap<String, Integer> b) {

        Set set = b.entrySet();
        Iterator i = set.iterator();
        while (i.hasNext()) {
            Map.Entry me = (Map.Entry) i.next();
            String newKey = (String) me.getKey();
            Integer newval = (Integer) me.getValue();

            Integer freq = (Integer) a.get(newKey);
            if (freq == null) {
                freq = newval;
            } else {
                int value = freq.intValue();
                freq = new Integer(value + newval.intValue());
            }
            a.put(newKey, freq);
        }
        return a;
    }
}
