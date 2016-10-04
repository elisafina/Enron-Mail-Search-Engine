/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.query.controller;

import com.indexing.controller.IndexCompression2;
import indexing.Indexing;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import query.QueryTerm;

/**
 *
 * @author irfannurhakim
 */
public class QueryProcessor {

    private static final String PREFIX_INDEX_FILENAME = QueryTerm.com + "inverted_index_";
    private static final String PREFIX_TERM_MAPPING_FILENAME = QueryTerm.com + "term_mapping_";
    private static final String DOC_MAPPING = "document_mapping.txt";
    private static HashMap<String, String> tempDocMapping = new HashMap<>();
    
    
    /**
     * Fungsi utama untuk melakukan pencarian
     * @param field
     * @param term
     * @param path
     * @throws IOException 
     */
    public static void doQuery(String field, String term, final String path) throws IOException {

        String indexFileName = path + PREFIX_INDEX_FILENAME + field + ".txt";
        dumpDocMapping(path);

        RandomAccessFile file = new RandomAccessFile(indexFileName, "r");
        ArrayList<Object> position = getPositionTerm(field, term, path);

        if (position.size() == 3) {
            file.seek((Long) position.get(0));
            byte[] buffer = new byte[(int) position.get(1) - Indexing.NEWLINE.getBytes().length];
            file.read(buffer);
            String str = new String(buffer);
            String content = str.split("=")[1];
            if (QueryTerm.isCompress) {
                String tests[] = content.split(";");
                String posID[] = tests[1].split(":");
                ArrayList<Integer> docID = IndexCompression2.StringToVByte(tests[0]);
                String temp = "";
                for (int i = 0; i < docID.size(); i++) {
                    temp += docID.get(i) + ":";
                    ArrayList<Integer> posIDs = IndexCompression2.StringToVByte(posID[i]);
                    String temp2 = "";
                    for (int j = 0; j < posIDs.size(); j++) {
                        temp2 += posIDs.get(j) + ",";
                    }
                    temp2 = temp2.substring(0, temp2.length() - 1);
                    temp += temp2 + ";";

                }
                content = temp;
            }
            String[] msgs = content.split(";");
            System.out.println(str);
            String toWrite = term + " FIELD=" + field + " DF=" + msgs.length + "\r\n";
            System.out.println(toWrite);
            List<Future<String>> list = new ArrayList<>();
            ExecutorService executor = Executors.newFixedThreadPool(100);
            
            
            for (final String string : msgs) {
                Callable<String> worker = new Callable<String>() {

                    @Override
                    public String call() throws Exception {
                        String[] a = string.split(":");
                        return "MSG ID=" + tempDocMapping.get(a[0]) + " TF=" + a[1].split(",").length + " " + a[1] + "\r\n";
                    }
                };
                Future<String> submit = executor.submit(worker);
                list.add(submit);
            }

            executor.shutdown();
            // Wait until all threads are finish
            while (!executor.isTerminated()) {
            }

            for (Future<String> future : list) {
                try {
                    toWrite += future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            writeFile(toWrite, field, term);
        } else {
            System.out.println("Term '" + term + "' Not Found in Field " + field);
        }
    }

    
    /**
     * Binary search untuk mencari lokasi dimana term yang dicari pada file index
     * @param field
     * @param term
     * @param path
     * @return
     * @throws IOException 
     */
    private static ArrayList<Object> getPositionTerm(String field, String term, String path) throws IOException {
        String termMappingFileName = path + PREFIX_TERM_MAPPING_FILENAME + field + ".txt";

        RandomAccessFile file = new RandomAccessFile(termMappingFileName, "r");
        ArrayList<Object> ret = new ArrayList<>();
        file.seek(0);
        String line = file.readLine().split("=")[0];
        if (line == null || line.compareTo(term) >= 0) {
            /*
             * the start is greater than or equal to the target, so it is what
             * we are looking for.
             */
            System.out.println(line);
            return null;
        }

        /*
         * set up the binary search.
         */
        long beg = 0;
        long end = file.length();
        boolean found = false;
        while (beg <= end) {
            /*
             * find the mid point.
             */
            long mid = beg + (end - beg) / 2;
            file.seek(mid);
            file.readLine();
            line = file.readLine().split("=")[0];
            if (line == null || line.compareTo(term) >= 0) {
                /*
                 * what we found is greater than or equal to the target, so look
                 * before it.
                 */
                if (line.matches(term)) {
                    found = true;
                }
                end = mid - 1;
            } else {
                /*
                 * otherwise, look after it.
                 */
                beg = mid + 1;
            }
        }

        if (found) {
            file.seek(beg);
            file.readLine();
            String target = file.readLine();
            ret.add(Long.parseLong(target.split("\\|")[1]));
            ret.add(Integer.valueOf(target.split("\\|")[2]));
            ret.add(target.split("=")[0]);
        }
        return ret;
    }

    /**
     * Fungsi untuk menulis hasil pencarian ke File
     * @param textToWrite
     * @param field
     * @param term
     * @throws IOException 
     */
    private static void writeFile(String textToWrite, String field, String term) throws IOException {
        try (FileChannel rwChannel = new RandomAccessFile(QueryTerm.com + Indexing.codeName + "-" + field + "-" + term + ".txt", "rw").getChannel()) {
            ByteBuffer wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, textToWrite.length());
            wrBuf.put(textToWrite.getBytes());
        }
    }

   /**
    * Fungsi untuk menyimpan document mapping kedalam hashmap
    * @param path
    * @throws IOException 
    */
    private static void dumpDocMapping(String path) throws IOException {
        String termMappingFileName = path + DOC_MAPPING;

        try {
            FileInputStream fstream = new FileInputStream(termMappingFileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            String[] a;
            while ((strLine = br.readLine()) != null) {
                a = strLine.split("="); 
                tempDocMapping.put(a[0], a[1]);
            }
            in.close();
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }
}
