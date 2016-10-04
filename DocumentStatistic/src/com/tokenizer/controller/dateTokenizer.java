/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tokenizer.controller;

import java.util.HashMap;

/**
 *
 * @author user
 */
public class dateTokenizer {

    /**
     * Author: Elisafina
     * memotong string date menjadi 5 bagian yaitu:
     * -hari : mon/tue/dll
     * -tanggal dalam format : 13dec2000
     * -jam dalam format : 16:39:00
     * - zona waktu : pdt/pst
     * - perbedaan waktu: -0700
     * @param date
     * @return hashmap yang berisi token dari date
     */
    public static HashMap<String, Integer> getListDate(String date) {
        HashMap<String, Integer> termList = new HashMap<String, Integer>();
        String[] terms = date.split(", | ");
        termList.put(terms[0], 1);
        termList.put(terms[1] + terms[2] + terms[3], 1);
        termList.put(terms[4], 1);
        termList.put(terms[5], 1);
        termList.put(terms[6], 1);
        //System.out.println(termList);
        return termList;
    }
}
