/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.indexing.model;

import com.indexing.controller.IndexCompression2;
import indexing.Indexing;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author user
 */
public class BigConcurentMap {


    public static ConcurrentSkipListMap<Long, String> dateConcurentMap = new ConcurrentSkipListMap<>();
    public static ConcurrentSkipListMap<Long, String> toConcurentMap = new ConcurrentSkipListMap<>();
    public static ConcurrentSkipListMap<Long, String> fromConcurentMap = new ConcurrentSkipListMap<>();
    public static ConcurrentSkipListMap<Long, String> subjectConcurentMap = new ConcurrentSkipListMap<>();
    public static ConcurrentSkipListMap<Long, String> bodyConcurentMap = new ConcurrentSkipListMap<>();
    public static ConcurrentSkipListMap<Long, String> allConcurentMap = new ConcurrentSkipListMap<>();

    /**
     * author: Elisafina 
     * untuk menggabungkan hashmap-hashmap kecil yang didapat
     * dari masing-masing file menjadi suatu concurentSkipListMap
     * besar, dan menyimpan mapping term dan termID ke dalam treeMap
     */
    public static void mergeBigHashMap(ConcurrentSkipListMap<Long, String> a, TreeMap<String, String> tree, HashMap<String, String> b, long docNumber) {
        // Get a set of the entries 
        //System.out.println(b);
        try {
            if (b != null) {
                Set set = b.entrySet();
                Iterator i = set.iterator();
                while (i.hasNext()) { // selama masih ada isi dalam hashmap kecil file
                    Map.Entry me = (Map.Entry) i.next();
                    String newKey = (String) me.getKey();
                    String newval = (String) me.getValue();
                    

                    Long idMapTerm = new Long(0);
                    // cek di dalam treeMap, jika belum ada tambahkan
                    String maping = tree.get(newKey);
                    while (maping == null) {
                        try {
                            tree.put(newKey, (tree.size() + 1) + "");
                            maping = tree.get(newKey);
                        } catch (Exception e) {
                            System.out.println(e.toString());
                            System.out.println(newKey);
                            System.out.println(tree.size());
                            maping = tree.get(newKey);
                            System.out.println(maping);
                            //e.printStackTrace();
                        }
                    }

                    //menambahkan hashmap<term, posisi> kecil ke dalam SkipListMap<termID, doc:posisi>
                    idMapTerm = Long.parseLong(maping);
                    String temp=(String) a.get(idMapTerm);
                    StringBuilder freq = new StringBuilder();
                    if ( temp== null) {
                            
                            freq.append(docNumber).append(":").append(newval.substring(0, newval.length() - 1)).append(";");
                         
                    } else {

                            freq=new StringBuilder(temp);
                            freq.append(docNumber).append(":").append(newval.substring(0, newval.length() - 1)).append(";");
                    }
                    a.put(idMapTerm, freq.toString());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * author: Elisafina 
     * untuk menulis bagian inverted index ke dalam file
     * setiap memproses 10ribu file inverted index (concurrentSkipListMap)
     * agar tidak membebani memori.
     * @param a
     * @param fileName
     */
    public static void printPartIndex(ConcurrentSkipListMap<Long, String> a, String fileName) {
        BufferedWriter fStream = null;
        try {
            fStream = new BufferedWriter(new FileWriter(fileName));
            Set set = a.entrySet();
            Iterator i = set.iterator();
            while (i.hasNext()) {
                Map.Entry me = (Map.Entry) i.next();
                Long newKey = (Long) me.getKey();
                String newval = (String) me.getValue();
                fStream.write(newKey + "=" + newval + Indexing.NEWLINE);

            }
            fStream.close();
            a.clear();
        } catch (IOException ex) {
            Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * author: Elisafina 
     * untuk membalik map yang asalnya key-value, menjadi value-key
     * @param map
     * @return
     */
    public static TreeMap<String, String>  reverseTree(TreeMap<String, String> map) {
    TreeMap<String, String> rev = new TreeMap<String, String>();
    for(Map.Entry<String, String> entry : map.entrySet())
        rev.put(entry.getValue(), entry.getKey());
    return rev;
}  

    /**
     * author: Elisafina 
     * untuk menggabungkan part-part inverted index yang dibuat
     * per 10rb file sehingga menjadi 1 inverted index utuh.
     * @param tree
     * @param filepart
     * @param index
     * @param indexMapping
     */
    public static void mergeInvertedIndex(TreeMap<String, String> tree, String filepart, BufferedWriter index, BufferedWriter indexMapping) {
        String fileparts[] = new String[Indexing.jumFile];
        BufferedReader raf[] = new BufferedReader[Indexing.jumFile];
        
        TreeMap<String, String> reverseTree =reverseTree(tree);
        
       // membuka koneksi ke buffered reader untuk semua file
        for (int i = 0; i < Indexing.jumFile; i++) {
            fileparts[i] = filepart +""+ (i + 1) + ".txt";
            try {
                raf[i] = new BufferedReader(new FileReader(fileparts[i]));
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }
        }
        String[] temp = new String[Indexing.jumFile];
        long[] tempInt = new long[Indexing.jumFile];

         //membaca line pertama dari semua part file
        for (int i = 0; i < Indexing.jumFile; i++) {
            try {
                temp[i] = raf[i].readLine();
                tempInt[i] = Long.parseLong(temp[i].split("=")[0]);
            } catch (IOException ex) {
                Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        // untuk setiap element dalam tree mapping
        long position =0;
        for (long i = 1; i < tree.size() + 1; i++) {
            StringBuilder tempString = new StringBuilder();
            int jumlahSama=0;
            for (int j = 0; j < Indexing.jumFile; j++) {
                //jika ada termID yang sama disatukan, kemudian part file yg memiliki
                //term ID yang sama tersebut akan membaca baru lagi.
                if (tempInt[j] == i) {
                    jumlahSama++;
                    tempString.append(temp[j].split("=")[1]);
                    try {
                        temp[j] = raf[j].readLine();
                        tempInt[j] = Long.parseLong(temp[j].split("=")[0]);
                    } catch (Exception ex) {
                        System.out.println("File ke " + j + "habis sampai " + i);
                        try {
                            raf[j].close();
                        } catch (IOException ex1) {
                            Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex1);
                        }
                    }

                }
            }
            // menulis hasil merging untuk 1 term ke file inverted index
            try {
                String key = reverseTree.get(i + "");
                if(i%10000==0)
                {
                    System.out.println(filepart+"-"+i);
                }
                String toWrite = i + "=" + tempString + Indexing.NEWLINE;
                long length = toWrite.getBytes().length;
                tree.put(key, i + "|" + position + "|" + length);
                position+=length;
                index.write(toWrite);
            } catch (IOException ex) {
                Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        try {
            index.close();
        } catch (IOException ex) {
            Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
        }
        // menuliskan term-termID mapping beserta posisi dan length
        Iterator<Map.Entry<String, String>> itr = tree.entrySet().iterator();
        while (itr.hasNext()) {
            try {
                Map.Entry<String, String> entry = itr.next();
                String term = entry.getKey();
                String indexTerm = entry.getValue();
                String toWrite = term + "=" + indexTerm + Indexing.NEWLINE;
                indexMapping.write(toWrite);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        try {
            indexMapping.close();
        } catch (IOException ex) {
            Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
        }
        tree.clear();
        //menghapus part file
        File f1;
        for (int i = 0; i < fileparts.length; i++) {
            try {
                raf[i].close();
            } catch (IOException ex) {
                Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
            }
            String string = fileparts[i];
            f1 = new File(string);
            if (f1.exists()) {
                f1.delete();
            }

        }
    }
}
