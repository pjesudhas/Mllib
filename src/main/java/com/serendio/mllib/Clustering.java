package com.serendio.mllib; /**
 * Created by praveen on 29/3/15.
 */
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.*;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.util.Arrays;

/**
 * Created by praveen on 23/3/15.
 */
class KlusterData {
    public Map<Integer,String> wordIndex = null;
    public Map<String,Double[]> featureMap = null;
    public Map<Integer,List<String>> clusterMap = null;
    public Map<Integer,String> labelMap =null;
    public KlusterData()
    {
        wordIndex = new HashMap<Integer, String>();
        featureMap = new HashMap<String, Double[]>();
        clusterMap = new HashMap<Integer,List<String>>();
        labelMap = new HashMap<Integer,String>();
    }
}

class groups
{
    public String label=null;
    public Integer weight=null;
    public List<groups> groups = null;
    public groups(){}
}

public class Clustering {
    public Configuration configuration;
    public FileSystem fileSystem;

    public List<String> getFilePathsFromFolder(String foldername)
    {
        List<String> filePaths = new ArrayList<String>();
        File rootDir = new File(foldername);
        for (File f : Files.fileTreeTraverser().preOrderTraversal(rootDir)) {
            if (f.isDirectory() == false) {
                filePaths.add(f.getAbsolutePath());
            }
        }
        return filePaths;
    }
    public Clustering() {
        configuration = new Configuration();
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public void storeFilesToHDFS(List<String> fileNames, String outputFolder, String documentsSequence)
    {
        Path documentsSequencePath = new Path(outputFolder, documentsSequence);
        try {
            SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, configuration, documentsSequencePath, Text.class, Text.class);

            for (String temp : fileNames) {
                String fileText = "";
                fileText = Files.toString(new File(temp), Charsets.UTF_8);
                writer.append(new Text(temp), new Text(fileText));
            }
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public void storeDBDataToHDFS(String caseno, String outputFolder, String documentsSequence)
    {
        DBlookup dbMan = new DBlookup();
        Map<String,String> docMap = dbMan.getDocumentsForCase(caseno);
        Path documentsSequencePath = new Path(outputFolder, documentsSequence);
        try {
            SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, configuration, documentsSequencePath, Text.class, Text.class);
            for (String temp : docMap.keySet())
            {
                writer.append(new Text(temp), new Text(docMap.get(temp)));
            }
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public void getTfIDFScoreForHDFSData(String outputFolder, String documentsSequence, String tfidf) {
        Path documentsSequencePath = new Path(outputFolder, documentsSequence);
        Path tokenizedDocumentsPath = new Path(outputFolder, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
        Path tfidfPath = new Path(outputFolder + tfidf);
        Path termFrequencyVectorsPath = new Path(outputFolder + DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
        try {
            DocumentProcessor.tokenizeDocuments(documentsSequencePath, StandardAnalyzer.class, tokenizedDocumentsPath, configuration);
            DictionaryVectorizer.createTermFrequencyVectors(tokenizedDocumentsPath, new Path(outputFolder), DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER,
                    configuration, 1, 1, 0.0f, PartialVectorMerger.NO_NORMALIZING, true, 1, 100, false, false);

            Pair<Long[], List<Path>> documentFrequencies = TFIDFConverter.calculateDF(termFrequencyVectorsPath, tfidfPath, configuration, 100);
            //System.out.println(documentFrequencies);
            TFIDFConverter.processTfIdf(termFrequencyVectorsPath, tfidfPath, configuration, documentFrequencies, 1, 100, PartialVectorMerger.NO_NORMALIZING, false, false, false, 1);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public Map<String, Double[]> getTFIDFScores(Path path)
    {
        SequenceFileIterable<Writable, Writable> iterable = new SequenceFileIterable<Writable, Writable>(path, configuration);
        Map<String, Map<Integer, Double>> tfIDFMap = new HashMap<String, Map<Integer, Double>>();
        Map<String, Double[]> finalftidf = new HashMap<String, Double[]>();
        Integer max = 0;
        for (Pair<Writable, Writable> pair : iterable) {
            String document = pair.getSecond().toString();
            String arr[] = document.replace("{", "").replace("}", "").replace(":", " ").split(",");
            Map<Integer, Double> tempMap = new HashMap<Integer, Double>();
            for (int i = 0; i < arr.length; i++) {
                String[] temArray = arr[i].split(" ");
                tempMap.put(Integer.parseInt(temArray[0].trim()), Double.parseDouble(temArray[1]));
                if (Integer.parseInt(temArray[0].trim()) > max)
                    max = Integer.parseInt(temArray[0].trim());
            }
            tfIDFMap.put(pair.getFirst().toString(), tempMap);
        }
        for (String temp : tfIDFMap.keySet()) {
            Double[] tfidfscore = new Double[max + 1];
            Arrays.fill(tfidfscore, 0.0);
            Map<Integer, Double> tempMap = tfIDFMap.get(temp);
            for (Integer wordPos : tempMap.keySet()) {
                tfidfscore[wordPos] = tempMap.get(wordPos);
            }
            finalftidf.put(temp, tfidfscore);
        }
        return finalftidf;
    }
    public void writePointsToFile(List<NamedVector> points,String fileName,FileSystem fs,Configuration conf)
    {
        Path path = new Path(fileName);
        try
        {
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, VectorWritable.class);
            VectorWritable vec = new VectorWritable();
            for (NamedVector point : points) {
                vec.set(point);
                writer.append(new Text(point.getName()), vec);
            }
            writer.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
    public List<NamedVector> getPoints(Map<String,Double[]> featureMap)
    {
        List<NamedVector> points = new ArrayList<NamedVector>();
        for(String temp : featureMap.keySet())
        {
            NamedVector vec = new NamedVector(new RandomAccessSparseVector(featureMap.get(temp).length),temp);
            vec.assign(ArrayUtils.toPrimitive(featureMap.get(temp)));
            points.add(vec);
        }
        return points;
    }
    public String formClusterJSON(KlusterData klust)
    {
        Gson gson = new GsonBuilder().create();
        groups groups = new groups();
        groups.groups = new ArrayList<groups>();
        for(Integer temp: klust.clusterMap.keySet())
        {
            List<String> fileList = klust.clusterMap.get(temp);
            groups tempgroups = new groups();
            tempgroups.label = klust.labelMap.get(temp);
            tempgroups.weight = fileList.size();
            tempgroups.groups = new ArrayList<groups>();
            for (String temp1 : fileList)
            {
                groups temp2 = new groups();
                temp2.label=temp1;
                tempgroups.groups.add(temp2);
            }
            groups.groups.add(tempgroups);
        }
        return gson.toJson(groups);
    }
    public String getJSONClusterForCase(String caseno)
    {
        Clustering obj = new Clustering();
        String outputFolder = "output/";
        String docSequence = "sequence";
        String tfidf = "tfidf";
        obj.storeDBDataToHDFS(caseno, outputFolder, docSequence);
        obj.getTfIDFScoreForHDFSData(outputFolder, docSequence, tfidf);
        KlusterData klust = new KlusterData();
        klust.featureMap =  obj.getTFIDFScores(new Path(outputFolder + "tfidf/tfidf-vectors/part-r-00000"));
        klust.wordIndex = obj.wordindex((new Path(outputFolder, "dictionary.file-0")));
        klust.clusterMap = obj.clusteringmethod(5, klust);
        klust.labelMap = obj.findClusterLabels(klust);

        try {
            HadoopUtil.delete(obj.configuration, new Path(outputFolder));
            HadoopUtil.delete(obj.configuration, new Path("clustering"));
        }catch(IOException ex)
        {
            ex.printStackTrace();
        }
        String jsonOut = obj.formClusterJSON(klust);
        return jsonOut;
    }
    public static void main(String args[])
    {
        if(args.length == 1)
        {
            String caseno = args[0];
            Clustering obj = new Clustering();
            String jsonOut = obj.getJSONClusterForCase(caseno);
            try {
                FileUtils.writeStringToFile(new File(caseno+"_out.txt"),jsonOut);
            }catch(IOException ex)
            {
                ex.printStackTrace();
            }
            DBlookup dbl = new DBlookup();
            dbl.storeCaseCluster(jsonOut,caseno);
            System.out.println(jsonOut);
        }
        else {
            System.out.println("Enter Case id");
        }
    }
    public Map<Integer,String> findClusterLabels(KlusterData klus)
    {
        Map<Integer,String> clusterLabels = new HashMap<Integer,String>();
        for(Integer clustemp : klus.clusterMap.keySet())
        {
            List<String> fileList = klus.clusterMap.get(clustemp);
            Double[] result = new Double[klus.wordIndex.size()];
            Arrays.fill(result,0.0);
            for(String temp : fileList)
            {
                Double[] tempv = klus.featureMap.get(temp);
                for(int i=0;i < tempv.length;i++)
                    result[i] = result[i] + tempv[i];
            }
            clusterLabels.put(clustemp,getClusterLabels(klus.wordIndex,result,5));
        }
        return clusterLabels;
    }
    String getClusterLabels(Map<Integer,String> wordIndex,Double[] resArray,int n)
    {
        String clusterLabels = "";
        for(int i=0; i < n; i++)
        {
            int index = 0;
            double max = 0;
            for(int j=0;j < resArray.length;j++)
            {
                if(max < resArray[j])
                {
                    max = resArray[j];
                    index = j;
                }
            }
            clusterLabels = clusterLabels + wordIndex.get(index) +" ";
            resArray[index] = 0.0;
        }
        return clusterLabels.trim();
    }
    public Map<Integer,String> wordindex(Path path) {
        Map<Integer,String> wordindex=new HashMap<Integer, String>();
        SequenceFileIterable<Writable, Writable> iterable = new SequenceFileIterable<Writable, Writable>(
                path, configuration);
        for (Pair<Writable, Writable> pair : iterable) {
            //System.out.format("%10s -> %s\n", pair.getFirst(), pair.getSecond());
            String key = pair.getFirst().toString();
            String value = pair.getSecond().toString();
            wordindex.put(Integer.parseInt(value),key);
        }
        return wordindex;
    }
    public Map<Integer,List<String>> clusteringmethod(int k,KlusterData klust)
    {
        Map<Integer,List<String>> clusteringmap = new HashMap<Integer,List<String>>();
        try
        {
            List<NamedVector> vectors = getPoints(klust.featureMap);
            File testData = new File("clustering/testdata");
            if (!testData.exists()) {
                testData.mkdir();
            }
            testData = new File("clustering/testdata/points");
            if (!testData.exists()) {
                testData.mkdir();
            }
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            writePointsToFile(vectors, "clustering/testdata/points/file1", fileSystem, configuration);

            Path path = new Path("clustering/testdata/clusters/part-00000");
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);

            for (int i = 0; i < k; i++)
            {
                org.apache.mahout.math.Vector vec = vectors.get(i);
                Kluster cluster = new Kluster(vec, i, new CosineDistanceMeasure());
                writer.append(new Text(cluster.getIdentifier()), cluster);
            }
            writer.close();
            KMeansDriver.run(conf,new Path("clustering/testdata/points"),new Path("clustering/testdata/clusters"),new Path("clustering/output"),0.01,100,true,0,true);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs,new Path("clustering/output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"), conf);

            IntWritable key = new IntWritable();
            WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();

            while (reader.next(key, value))
            {
                NamedVector vec = (NamedVector)value.getVector();
               // System.out.println(vec.getName());

                if(clusteringmap.containsKey(key.get()) == true)
                {
                    clusteringmap.get(key.get()).add(vec.getName());
                }
                else
                {
                    List<String> temp1 = new ArrayList<String>();
                    temp1.add(vec.getName());
                    clusteringmap.put(key.get(),temp1);
                }
            }
            reader.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        return clusteringmap;
    }
}