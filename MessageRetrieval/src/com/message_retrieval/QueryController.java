/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.message_retrieval;




import java.io.*;
import java.util.Map.Entry;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author user
 */
public class QueryController {

    private static final String PREFIX_INDEX_FILENAME = MainQuery.com + "inverted_index_";
    private static final String PREFIX_TERM_MAPPING_FILENAME = MainQuery.com + "term_mapping_";
    private static final String DOC_MAPPING = "document_mapping.txt";
    //konstanta-konstanta yang digunakan pada BM25
    final static double k1 = 1.2;
    final static double b = 0.75;
    final static double k2 = 100;

    /**
     * @author: Irfan
     * Fungsi untuk mendapatkan message id dan statistik panjang dari setiap
     * dokumen dan menympimpan ke dalam bentuk Hasmap yang berisi pasangan
     * <DocID, (messID,panjang doc)>
     *
     * @return HashMap<String, DocMappingModel>
     */
    public static HashMap<String, DocMappingModel> getDocMapping() {
       
        long[] tempLength = {0, 0, 0, 0, 0, 0};
        String path = MainQuery.path;
        String termMappingFileName = path + DOC_MAPPING;
        HashMap<String, DocMappingModel> temp = new HashMap<>();

        try {
            FileInputStream fstream = new FileInputStream(termMappingFileName);
            try (DataInputStream in = new DataInputStream(fstream)) {
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String strLine;
                String[] a;
                while ((strLine = br.readLine()) != null) {
                    a = strLine.replaceAll("=", "\\|").split("\\|");
                    String[] b = a[2].split(",");
                    int[] c = new int[b.length];
                    for (int i = 0; i < b.length; i++) {
                        c[i] = Integer.valueOf(b[i]);
                        tempLength[i] += c[i];
                    }
                    temp.put(a[0], new DocMappingModel(a[1], c));
                }
            }
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
        //menyimpan rata-rata panjang dokumen untuk setiap field, diperlukan
        //pada algoritma BM25
        for (int i = 0; i < tempLength.length; i++) {
            MainQuery.avgDocLength[i] = tempLength[i] * 1.00 / temp.size();
        }
        return temp;
    }

    
    /**
     * author: irfan
     * menghasilkan hashmap yang berisi <term:field,Queryfreq> dimana term merupakan 
     * kata yang dicari, field merupakan di field mana kata tersebut dicari (dalam kode 1-6)
     * dan queryFreq merupakan berapa banyak query tersebut muncul
     * @param query
     * @return
     */
    public static HashMap<String, Integer> queryNormalization(String query) {
        
        HashMap<String, Integer> res = new HashMap<>();
        
        ArrayList<String>  yy = queryDestroyer(query);
        //System.out.println(yy);
        for (Iterator<String> it = yy.iterator(); it.hasNext();) {
            String string = it.next();
            putToHashMap(string, res);
            
        }
         
        return res;
    }

    /**
     * author: irfan
     * Fungsi untuk memecah query menjadi sekumpulan string berdasarkan query
     * dan field kemudian disimpan ke dalam array list of string.
     * contoh hasil output:[saya:5, saya:4, makan|nasi:6, tono:6, susi:1]
     * @param query
     * @return HashMap
     */
    public static ArrayList<String> queryDestroyer(String query) {
        ArrayList<String> res = new ArrayList<>();
        String[] yy = query.split("\\s");
        for (int i = 0; i < yy.length; i++) {
            String temp = yy[i];
            if (yy[i].startsWith("\"")) {
                for (int j = i + 1; j < yy.length; j++) {
                    temp += "|" + yy[j];
                    i++;
                    if (yy[j].contains("\"")) {
                        break;
                    }
                }
            }
            String[] xx = temp.split(":");
            if (xx.length == 1) {
                //res.put(temp, 6);
                //res.add(temp.replaceAll("\"", "")+":"+6);
                res.add(temp.replaceAll("\"", "") +":"+6);
            } else {
                //res.put(xx[0].replaceAll("\"", ""), fieldTransform(xx[1]));
                res.add(xx[0].replaceAll("\"", "")+":"+fieldTransform(xx[1]));
            }
        }
        return res;
    }

    /**
     * author: irfan
     * Fungsi untuk mentransformasi nama field menjadi kode field
     *
     * @param field
     * @return
     */
    public static Integer fieldTransform(String field) {
        int code;
        switch (field) {
            case "date":
                code = 1;
                break;
            case "to":
                code = 2;
                break;
            case "from":
                code = 3;
                break;
            case "subject":
                code = 4;
                break;
            case "body":
                code = 5;
                break;
            default:
                code = 6;
                break;
        }

        return code;

    }

    /**
     * author: irfan 
     * method untuk memasukan sebuah key ke hashmap dengan
     * pengecekan, jika key tersebut sudah pernah ada maka value-nya akan
     * ditambah 1
     *
     * @param key
     * @param map
     */
    public static void putToHashMap(String key, HashMap<String, Integer> map) {
        Integer freq = (Integer) map.get(key);
        if (freq == null) {
            freq = new Integer(1);
        } else {
            int value = freq.intValue();
            freq = new Integer(value + 1);
        }
        map.put(key, freq);
    }

    /**
     * author: elisafina
     * Fungsi untuk mendapatkan document length untuk docID tertentu
     * dan field tertentu.
     * @param docID
     * @param field
     * @return
     */
    public static int fieldLengthAcc(String docID, int field) {
        int hasil = 0;
        DocMappingModel doc = MainQuery.docMapping.get(docID);
        for (int i = 0; i < field - 1; i++) {
            hasil += doc.docLength[i];
        }
        return hasil;
    }

    /**
     * author:irfan
     * Fungsi untuk menyimpan dictionary termMapping 
     * kedalam hashmap di memory
     *
     * @param path
     * @throws IOException
     */
    public static HashMap<String, String> dumpTermMapping(String path) throws IOException {
        HashMap<String, String> res = new HashMap<>();
        try {
            FileInputStream fstream = new FileInputStream(path);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            String[] a;
            while ((strLine = br.readLine()) != null) {
                a = strLine.split("=");
                res.put(a[0], a[1]);
            }
            in.close();
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }

        return res;
    }

    /**
     * author:irfan
     * Fungsi untuk mengambil semua termMapping untuk semua field
     *
     * @return
     */
    public static ArrayList<HashMap<String, String>> getTermMapping() {
        ArrayList<HashMap<String, String>> hasil = new ArrayList<>();
        String field;
        for (int i = 1; i < 6; i++) {
            switch (i) {
                case 1:
                    field = "date";
                    break;
                case 2:
                    field = "to";
                    break;
                case 3:
                    field = "from";
                    break;
                case 4:
                    field = "subject";
                    break;
                case 5:
                    field = "body";
                    break;
                default:
                    field = "body";
                    break;
            }

            String path = MainQuery.path;
            // String indexFileName = path +MainQuery.com+ QueryProcessor.PREFIX_INDEX_FILENAME + field + ".txt";
            String termMappingFileName = path + PREFIX_TERM_MAPPING_FILENAME + field + ".txt";
            try {
                HashMap<String, String> termTemp = dumpTermMapping(termMappingFileName);
                hasil.add(termTemp);
            } catch (IOException ex) {
                Logger.getLogger(QueryController.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return hasil;
    }

    /**
     * author: elisafina
     * Fungsi untuk mendapatkan posting list dari semua term yang diberikan (query)
     * sesuai dengan field yang diminta. kembaliannya berupa hashmap<docID,array list of position>
     *
     * @param term
     * @param fieldCode
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static HashMap<String, ArrayList<Integer>> getPostingList(String term, int fieldCode) throws FileNotFoundException, IOException {
       
        String field;
        HashMap<String, ArrayList<Integer>> temp = new HashMap<>();
        if (fieldCode == 6) {
            temp = getPostingListAll(term);
        } else {
            switch (fieldCode) {
                case 1:
                    field = "date";
                    break;
                case 2:
                    field = "to";
                    break;
                case 3:
                    field = "from";
                    break;
                case 4:
                    field = "subject";
                    break;
                case 5:
                    field = "body";
                    break;
                default:
                    field = "body";
                    break;
            }


            String path = MainQuery.path;
            String indexFileName = path +  PREFIX_INDEX_FILENAME + field + ".txt";
//            String termMappingFileName = path + QueryProcessor.PREFIX_TERM_MAPPING_FILENAME + field + ".txt";
//            
//            HashMap<String, String> termTemp = dumpTermMapping(termMappingFileName);
            HashMap<String, String> termTemp = MainQuery.termMapping.get(fieldCode - 1);
            String strFromHashMap = termTemp.get(term);
            ArrayList<Object> position = new ArrayList<>();

            if (strFromHashMap != null) {
                String[] poss = strFromHashMap.split("\\|");
                position.add(Long.parseLong(poss[1]));
                position.add(Integer.valueOf(poss[2]));
                position.add(strFromHashMap.split("=")[0]);

                RandomAccessFile indexFile = new RandomAccessFile(indexFileName, "r");
                indexFile.seek((Long) position.get(0));
                byte[] buffer = new byte[(int) position.get(1) - "\r\n".getBytes().length];
                indexFile.read(buffer);
                String str = new String(buffer);
                //System.out.println(str);
                String content = str.split("=")[1];
                if (MainQuery.isCompress) {
                    //System.out.println("masuk sini");
                    String tests[] = content.split(";");
                    String posID[] = tests[1].split(":");
                    ArrayList<Integer> docID = IndexCompression2.StringToVByte(tests[0]);
                    StringBuilder tempss = new StringBuilder("");
                    for (int i = 0; i < docID.size(); i++) {
                        tempss.append(docID.get(i) + ":");
                        //tempss += docID.get(i) + ":";
                        ArrayList<Integer> posIDs = IndexCompression2.StringToVByte(posID[i]);
                        StringBuilder temp2 = new StringBuilder("");
                        for (int j = 0; j < posIDs.size(); j++) {
                            //temp2 += posIDs.get(j) + ",";
                            temp2.append(posIDs.get(j) + ",");
                        }
                        String temp3 = temp2.toString();
                        temp3 = temp3.toString().substring(0, temp3.length() - 1);
                        //tempss += temp2 + ";";
                        tempss.append(temp2 + ";");

                    }
                    content = tempss.toString();
                    //  System.out.println(content);
                }
                String[] msgs = content.split(";");

                for (String docs : msgs) {
                    String[] pos = docs.split(":");
                    String docID = pos[0];
                    String[] posisi = pos[1].split(",");
                    ArrayList<Integer> tempPos = new ArrayList<>();
                    for (String posTerm : posisi) {
                        tempPos.add(Integer.valueOf(posTerm));
                    }
                    temp.put(docID, tempPos);
                }
            }
        }
        return temp;
    }

    /**
     *
     * author: elisafina
     * method untuk mengambil posting list untuk field all..
     * karena pada index tidak terdapat index untuk field all, maka untuk mendapatkan posting list 
     * untuk field all, dilakukan dengan cara mengambil posting list untuk semua field (date sampai body)
     * kemudian menggabungkan dan meng-update posisi sesuai dengan posisi relative terhadap semua isi dokumen
     * misalkan posisi 2 pada field subject, maka posisi untuk all adalah panjang date+panjang from + panjang 
     * to + 2.
     * @param term
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static HashMap<String, ArrayList<Integer>> getPostingListAll(String term) throws FileNotFoundException, IOException {
        HashMap<String, ArrayList<Integer>> hasil = new HashMap<>();
        ArrayList<HashMap<String, ArrayList<Integer>>> temp = new ArrayList<>();

        for (int i = 1; i < 6; i++) {
            temp.add(getPostingList(term, i));
        }

        for (int i = 0; i < temp.size(); i++) {

            HashMap<String, ArrayList<Integer>> pos = temp.get(i);
            if (pos != null) {
                //System.out.println(pos);
                Iterator<Entry<String, ArrayList<Integer>>> itr = pos.entrySet().iterator();
                while (itr.hasNext()) {
                    Entry<String, ArrayList<Integer>> entry = itr.next();
                    String docID = entry.getKey();
                    ArrayList<Integer> docPos = entry.getValue();
                    ArrayList<Integer> docPosOld = hasil.get(docID);
                    if (docPosOld == null) {
                        docPosOld = new ArrayList<>();
                    }
                    for (int j = 0; j < docPos.size(); j++) {
                        Integer posID = docPos.get(j);
                        posID += fieldLengthAcc(docID, i + 1);
                        docPosOld.add(posID);
                    }
                    hasil.put(docID, docPosOld);
                }
            }
        }

        return hasil;
    }

    /**
     * Author : elisafina
     * mengambil semua posting list untuk field yang sesuai untuk query term dan field yang diberikan
     * kemudian dimasukkan ke dalam allPostingList. Kemudian untuk setiap dokumen ID yang ditemukan 
     * akan dimasukkan ke dalam HashSet allDocID karena tipe nya hashset maka tidak akan terjadi 
     * duplikasi docID. allPostList dan alldocID ini yang akan digunakan pada proses ranking.
     * Yang disimpan dalam allPostList adalah HashMap<term:field, Hashmap<docID,TF>>.
     * 
     * @param termField
     * @param allPostList
     * @param allDocID
     */
    public static void getPostingListBig(String termField, HashMap<String, HashMap<String, Integer>> allPostList,
            HashSet<String> allDocID) {
        HashMap<String, Integer> postListFreq = new HashMap<>();
        HashMap<String, ArrayList<Integer>> postList = null;
        try {
            String[] xx = termField.split(":");
            postList = getPostingList(xx[0], Integer.parseInt(xx[1]));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(QueryController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(QueryController.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (postList != null) {
            Iterator<Entry<String, ArrayList<Integer>>> itr = postList.entrySet().iterator();
            while (itr.hasNext()) {
                Entry<String, ArrayList<Integer>> entry = itr.next();
                String docID = entry.getKey();
                ArrayList<Integer> docPos = entry.getValue();
                allDocID.add(docID);
                postListFreq.put(docID, docPos.size());
            }
            allPostList.put(termField, postListFreq);
        }
        //System.out.println(allDocID);

    }

    /**
     *
     * Author : elisafina
     * mengambil semua posting list untuk field yang sesuai untuk query term dan field yang diberikan
     * kemudian dimasukkan ke dalam allPostingList. Kemudian untuk setiap dokumen ID yang ditemukan 
     * akan dimasukkan ke dalam HashSet allDocID karena tipe nya hashset maka tidak akan terjadi 
     * duplikasi docID. allPostList dan alldocID ini yang akan digunakan pada proses ranking.
     * Yang disimpan dalam allPostList adalah HashMap<term:field, Hashmap<docID,TF>>.
     * perbedaan dengan method sebelumnya adalah method ini digunakan khusus untuk query dengan 
     * tanda "....", sehingga dalam perhitungan termFrekuensi harus diperhatikan dahulu apakah
     * posisinya berurutan.
     * @param termField
     * @param allPostList
     * @param allDocID
     */
    public static void getPostingListBigSequence(String termField, HashMap<String, HashMap<String, Integer>> allPostList,
            HashSet<String> allDocID) {

        HashMap<String, Integer> hasil = new HashMap<>();
        HashSet<String> docIDS = new HashSet<>();
        ArrayList<HashMap<String, ArrayList<Integer>>> postList = new ArrayList<>();
        String[] xx = termField.split(":");
        String term =xx[0];
        int field =Integer.parseInt(xx[1]);
        String terms[] = term.split("\\|");
        for (String string : terms) {
            try {
                //System.out.println(string);
                HashMap<String, ArrayList<Integer>> temp = getPostingList(string, field);
                //System.out.println(temp);
                Set<String> doc = temp.keySet();
                docIDS.addAll(doc);
                postList.add(temp);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(QueryController.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(QueryController.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        Iterator iter = docIDS.iterator();
        while (iter.hasNext()) {
            String docID = (String) iter.next();
            boolean allHaveDoc = true;
            int counter = 0;
            for (int i = 0; i < postList.size(); i++) {
                HashMap<String, ArrayList<Integer>> postID = postList.get(i);
                if (!postID.containsKey(docID)) {
                    allHaveDoc = false;
                }
            }
            if (allHaveDoc) {
                ArrayList<Integer> root = postList.get(0).get(docID);

                for (int i = 0; i < root.size(); i++) {
                    int posRoot = root.get(i);
                    boolean isSequence = true;
                    for (int j = 1; j < postList.size(); j++) {
                        ArrayList<Integer> check = postList.get(j).get(docID);
                        if (!check.contains(posRoot + j)) {
                            isSequence = false;
                        }
                    }
                    if (isSequence) {
                        counter++;
                    }
                }
                if (counter != 0) {
                    allDocID.add(docID);
                    hasil.put(docID, counter);
                }
            }
        }
        allPostList.put(termField, hasil);
    }

    /**
     * Author : elisafina
     * merupakan method gabungan yang didalamnya memanggil method untuk mengambil posting list
     * untuk setiap query term yang diberikan dan kemudian menghitung nilai bobot ranking setiap dokumen
     * dan hasilnya ditampung dalam hashmap
     * @param query
     * @param docMapping
     * @param avgDocLength
     * @return
     */
    public static LinkedHashMap<String, Double> getWeight(HashMap<String, Integer> query,
            HashMap<String, DocMappingModel> docMapping,
            double[] avgDocLength) {
        HashMap<String, Double> hasils = new HashMap<>();
        HashMap<String, HashMap<String, Integer>> allPostList = new HashMap<>();
        HashSet<String> allDocID = new HashSet<>();
        
        //mengambil semua posting list untuk semua query term yang diberikan
        Iterator<Entry<String, Integer>> itr = query.entrySet().iterator();
        while (itr.hasNext()) {
            Entry<String, Integer> entry = itr.next();
            String termField=entry.getKey();
            String xx[] = termField.split(":", 2);
            String term=xx[0];
            int field = Integer.parseInt(xx[1]);
            
            //System.out.println(term);
            if (term.indexOf("|") != -1) {
                //System.out.println(term);
                getPostingListBigSequence(termField, allPostList, allDocID);
            } else {
                getPostingListBig(termField, allPostList, allDocID);
            }
        }
        //System.out.println(allDocID.size());

        //menghitung bobot ranking setiap dokumen
        hasils = calculateRank(query, allPostList, allDocID, docMapping, avgDocLength);
        return sortByValue(hasils);
    }

    /**
     * author: pandapotan
     * fungsi yang digunakan untuk menghitung bobot ranking dokumen hasil
     * dari pengambilan posting list.
     * @param query
     * @param allPostList
     * @param allDocID
     * @param docMapping
     * @param avgDocLength
     * @return
     */
    public static HashMap<String, Double> calculateRank(HashMap<String, Integer> query, HashMap<String, HashMap<String, Integer>> allPostList,
            HashSet<String> allDocID, HashMap<String, DocMappingModel> docMapping,
            double[] avgDocLength) {
        HashMap<String, Double> hasil = new HashMap<>();

        int totalDoc = docMapping.size();
        Iterator it = allDocID.iterator();

        while (it.hasNext()) {
            double BM25Score = 0;
            String docID = (String) it.next();
            DocMappingModel docMod = docMapping.get(docID);
            

            Iterator<Entry<String, HashMap<String, Integer>>> itr = allPostList.entrySet().iterator();
            while (itr.hasNext()) {
                Entry<String, HashMap<String, Integer>> entry = itr.next();
                String termField = entry.getKey();
                //System.out.println(termField);
                String xx[] = termField.split(":");
                String term = xx[0];
                int field = Integer.parseInt(xx[1]);
                HashMap<String, Integer> postList = entry.getValue();
                int docLength = (int) docMod.getDocLength()[field - 1];
                double avgDoc = avgDocLength[field - 1];
                int qf = query.get(termField);
                int docFreq = postList.size();
                int termFreq = 0;
                if (postList.get(docID) != null) {
                    termFreq = postList.get(docID);
                }
                BM25Score += BM25Calculator(docLength, avgDoc, docFreq, termFreq, totalDoc, qf);
            }

            hasil.put(docMod.messID, BM25Score);
        }


        //System.out.println(hasil);
        //ngitung pke allpostList sama alldoc, pke algo BM25
        return hasil;
    }

    /**
     * Author: pandapotan
     * implementasi algoritma ranking BM25
     * @param docLength
     * @param avgDocLength
     * @param docFreq
     * @param termFreq
     * @param totalDoc
     * @param queryFreq
     * @return
     */
    public static double BM25Calculator(int docLength, double avgDocLength, int docFreq,
            int termFreq, int totalDoc, int queryFreq) {
        double hasil = 0.0;
        double persen = docLength * 1.0 / avgDocLength;
        //System.out.println(persen);
        double K = k1 * ((1 - b) + (b * persen));

        double a1 = Math.log((0.5 / 0.5) / ((docFreq + 0.5) / (totalDoc - docFreq + 0.5)));
        //System.out.println(a1);
        double a2 = ((k1 + 1) * termFreq) / (K + termFreq);
        //System.out.println(a2);
        double a3 = ((k2 + 1) * queryFreq) / (k2 + queryFreq);
        //System.out.println(a3);
        hasil = a1 * a2 * a3;

        return hasil;
    }

    /**
     * author: pandapotan
     * method yang digunakan untuk mengurutkan hasil ranking
     * dari yang bobotnya tertinggi sampai terendah.
     * @param map
     * @return
     */
    public static LinkedHashMap sortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {

            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
            }
        });

        LinkedHashMap result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * author: pandapotan
     * method yang digunakan untuk menuliskan hasil ranking yang didapat ke dalam file
     * @param query
     * @param weight
     * @param fileName
     * @throws IOException
     */
    public static void printWeight(String query, LinkedHashMap<String, Double> weight, String fileName) throws IOException {
        BufferedWriter weightFile = new BufferedWriter(new FileWriter(fileName));
        Iterator<Entry<String, Double>> itr = weight.entrySet().iterator();
        int rank = 0;
        MainQuery.end = System.currentTimeMillis();
        long time = MainQuery.end - MainQuery.start;
        //System.out.println("time:" + (end - start) * 1.00 / 1000);
        weightFile.write(query + MainQuery.NEWLINE);
        while (itr.hasNext() && rank < 40) {
            try {
                rank++;
                Entry<String, Double> entry = itr.next();
                String docID = entry.getKey();
                Double bm = entry.getValue();
                //indexMapping.seek(indexMapping.length());
                String toWrite = docID + " " + rank + " " + bm + MainQuery.NEWLINE;
                weightFile.write(toWrite);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        weightFile.write("Execution time : " + time + " miliseconds");
        weightFile.close();
    }

    public static void main(String[] args) {

        //double k = BM25Calculator(90, 100, 300, 25, 500000, 1);
        //System.out.println(k);
        System.out.println(queryDestroyer("saya:body saya:subject \"makan nasi\" tono susi:date"));
        System.out.println(queryNormalization("raptor:subject oct:date transactions:body purchased \"Ron Baker\""));
    }
}
