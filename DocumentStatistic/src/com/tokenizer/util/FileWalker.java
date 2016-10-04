
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tokenizer.util;

import com.tokenizer.model.BigConcurentHashMap;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import tokenizer.Tokenizer;

/**
 *
 * @author irfannurhakim
 */
public class FileWalker extends SimpleFileVisitor<Path> {

    private static long startTime;
    private int nrOfProcessors = Runtime.getRuntime().availableProcessors();
    private ExecutorService es = Executors.newFixedThreadPool(nrOfProcessors);
    private Runtime rt = Runtime.getRuntime();
    private int i = 0;

    public FileWalker() {
        startTime = System.nanoTime();
    }

    /**
     * Visit each files in directory and after that submit a job to worker to
     * read the file.
     *
     * @param aFile
     * @param aAttrs
     * @return enumerator
     * @throws IOException
     */
    @Override
    public FileVisitResult visitFile(
            Path aFile, BasicFileAttributes aAttrs) throws IOException {
        /**
         * skip .DS_Store file
         * Print every 1000 jobs send to worker
         */
        if (!aFile.getFileName().toString().equalsIgnoreCase(".DS_Store")) {
            if (i % 1000 == 0) {
                System.out.println("job send " + i);
            }
            i++;
            /**
             * Instantiate the worker
             * send job to worker with parameter directory file and job counter
             */
            FileReader task = new FileReader(aFile, i);
            task.setCaller(this);
            es.submit(task);
        }
        return FileVisitResult.CONTINUE;
    }

    /**
     * This method visit all directory, after end with a file then return
     * enumeration with CONTINUE value to read the file skip for all_documents
     * folder
     *
     * @param aDir
     * @param aAttrs
     * @return
     * @throws IOException
     */
    @Override
    public FileVisitResult preVisitDirectory(
            Path aDir, BasicFileAttributes aAttrs) throws IOException {
        if (aDir.endsWith("all_documents/")) {
            return FileVisitResult.SKIP_SUBTREE;
        }
        return FileVisitResult.CONTINUE;
    }

    /**
     * After a job has finished by the worker, worker will call this method to
     * acknowledge that the job has done When the total number of finished job
     * equal to total number job that has been submitted, the system will
     * terminate and give a statistical result in a text file
     *
     * @param date
     * @param from
     * @param to
     * @param subject
     * @param body
     * @param allFieldMap
     * @param jobDone
     * @throws InterruptedException
     */
    public void callback(HashMap<String, Integer> date, HashMap<String, Integer> from, HashMap<String, Integer> to, HashMap<String, Integer> subject, HashMap<String, Integer> body, HashMap<String, Integer> allFieldMap, int jobDone) throws InterruptedException {
        /**
         * Print each 1000 executed job, and run the garbage collector for memory efficiency
         */
        if (jobDone % 1000 == 0) {
            System.out.println("<==> job done " + jobDone + " from: " + i + " in : " + ((System.nanoTime() - startTime) / 1000000000.0) + " secs");
            rt.gc();
            rt.gc();
        }
        
        /**
         * Terminate job while all job has finished
         */
        if (jobDone >= i) {
            es.shutdown();
            es.awaitTermination((long) 100, TimeUnit.MILLISECONDS);
            
            
            /**
             * Calculate all document statistics and print to file
             */ 
            Tokenizer.N_messagge = i;
            try {
                LinkedHashMap dateList = BigConcurentHashMap.calculateTermWight(BigConcurentHashMap.dateConcurentMap, Tokenizer.N_messagge);
                BigConcurentHashMap.printStatistic(dateList, "date", Tokenizer.N_messagge);
                LinkedHashMap fromList = BigConcurentHashMap.calculateTermWight(BigConcurentHashMap.fromConcurentMap, Tokenizer.N_messagge);
                BigConcurentHashMap.printStatistic(fromList, "from", Tokenizer.N_messagge);
                LinkedHashMap toList = BigConcurentHashMap.calculateTermWight(BigConcurentHashMap.toConcurentMap, Tokenizer.N_messagge);
                BigConcurentHashMap.printStatistic(toList, "to", Tokenizer.N_messagge);
                LinkedHashMap subjectList = BigConcurentHashMap.calculateTermWight(BigConcurentHashMap.subjectConcurentMap, Tokenizer.N_messagge);
                BigConcurentHashMap.printStatistic(subjectList, "subject", Tokenizer.N_messagge);
                LinkedHashMap bodyList = BigConcurentHashMap.calculateTermWight(BigConcurentHashMap.bodyConcurentMap, Tokenizer.N_messagge);
                BigConcurentHashMap.printStatistic(bodyList, "body", Tokenizer.N_messagge);
                LinkedHashMap allList = BigConcurentHashMap.calculateTermWight(BigConcurentHashMap.allConcurentMap, Tokenizer.N_messagge);
                BigConcurentHashMap.printStatistic(allList, "all", Tokenizer.N_messagge);
            } catch (Exception e) {
                e.printStackTrace();
            }
             System.out.println("Total execution time : " + ((System.nanoTime() - startTime) / 1000000000.0) + " secs");
            System.exit(0);
        }

    }
}
