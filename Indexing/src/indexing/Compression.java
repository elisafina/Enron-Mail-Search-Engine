/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexing;

import com.indexing.controller.IndexCompression2;
import com.indexing.model.BigConcurentMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author user
 */
public class Compression {

    static String fieldName = "subject";
    static BufferedReader  invertedIndex = null;
    static BufferedReader  indexMapping = null;
    static BufferedWriter  cominvertedIndex = null;
    static BufferedWriter  comindexMapping = null;
    static LinkedHashMap<String, String> map = new LinkedHashMap<>();
    public final static String NEWLINE = "\r\n";

    public static void main(String[] args) {
        try {
            //String pathasal="C:\\Users\\user\\Desktop\\Inverted_Index\\";
            //String pathtujuan="C:\\Users\\user\\Desktop\\Inverted_Index_compressed\\";
            if(args.length != 2)
          { 
              System.out.println("Usage: Compression <inverted_index_location> <field_name> ");
              System.exit(0);
          }
            
            String pathasal=args[0];
            fieldName= args[1];
            invertedIndex = new BufferedReader (new FileReader(pathasal+"inverted_index_" + fieldName + ".txt"));
            indexMapping = new BufferedReader (new FileReader(pathasal+"term_mapping_" + fieldName + ".txt"));
            cominvertedIndex = new BufferedWriter(new FileWriter(pathasal+"com_inverted_index_" + fieldName + ".txt"));
            comindexMapping = new BufferedWriter(new FileWriter(pathasal+"com_term_mapping_" + fieldName + ".txt"));


            String mapping;
            try {
                mapping = indexMapping.readLine();
                while (mapping != null) {
                    //System.out.println(mapping);
                    String raw[] = mapping.split("=");
                    String term = raw[0];
                    String termID = raw[1].split("\\|")[0];
                    map.put(termID, term);
                    mapping = indexMapping.readLine();
                }
                //System.out.println(map);
            } catch (IOException ex) {
                Logger.getLogger(Compression.class.getName()).log(Level.SEVERE, null, ex);
            }

            String indexing;

            long position = 0;
            try {
                indexing = invertedIndex.readLine();
                
                while (indexing != null) {
                    String raws[] = indexing.split("=");
                    String ID = raws[0];

                    
                    String hasil = compressPostingList(indexing) + NEWLINE;
                    long length = hasil.getBytes().length;
                    String value = map.get(ID);
                    //System.out.println(ID);
                    map.put(ID, value + "|" + position + "|" + length);
                     cominvertedIndex.write(hasil);
                    position += length;
                   
                    indexing = invertedIndex.readLine();
                }
                cominvertedIndex.close();


                
                Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
        while (itr.hasNext()) {
            try {
                Map.Entry<String, String> entry = itr.next();
                String termID = entry.getKey();
                String raw[] = entry.getValue().split("\\|");
                //comindexMapping.seek(comindexMapping.length());
                try {
                     String toWrite = raw[0] + "=" + termID+"|"+raw[1]+"|"+raw[2]+ Indexing.NEWLINE;
                    comindexMapping.write(toWrite);
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println(entry.getValue()+"|"+entry.getKey());
                }
               
            } catch (IOException ex) {
                Logger.getLogger(BigConcurentMap.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        comindexMapping.close();
        indexMapping.close();
        invertedIndex.close();
                //System.out.println(map);
            } catch (IOException ex) {
                Logger.getLogger(Compression.class.getName()).log(Level.SEVERE, null, ex);
            }


        } catch (Exception ex) {
            Logger.getLogger(Compression.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static String compressPostingList(String indexing) {
        TreeMap<Integer, ArrayList<Integer>> index = new TreeMap<>();
        String raw[] = indexing.split("=");
        String termID = raw[0];
        String hasil;
        //System.out.println(termID);
        try {
            
             String postingList[] = raw[1].split(";");
        for (String post : postingList) {
            String temp[] = post.split(":");
            int docID = Integer.parseInt(temp[0]);
            String pos[] = temp[1].split(",");
            ArrayList<Integer> position = new ArrayList<>();
            for (String poss : pos) {
                position.add(Integer.parseInt(poss));
            }
            index.put(docID, position);

        }
        //System.out.println(index);

        ArrayList<Integer> docIDs = new ArrayList<>(index.keySet());
        //System.out.println(docIDs);
        String compresDocIDs = IndexCompression2.VByteToString(new LinkedList<>(docIDs));
        StringBuilder compressPos = new StringBuilder();
        Iterator<Map.Entry<Integer, ArrayList<Integer>>> iter = index.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, ArrayList<Integer>> entry = iter.next();
            ArrayList<Integer> pos = entry.getValue();
            compressPos.append(IndexCompression2.VByteToString(new LinkedList<>(pos))).append(":");
        }
        hasil = compresDocIDs + ";" + compressPos.substring(0, compressPos.length() - 1);
        } catch (Exception e) {
          hasil="";
        }
       
        //System.out.println(hasil);
        return termID + "=" + hasil;
    }
}
