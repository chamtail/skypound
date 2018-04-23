package vip.theo.skypound;

import com.alibaba.fastjson.JSON;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataReader {
    /**
     * 最简陋版本		1055
     * 阴性=0 阳性=1	1507
     * 无=0			1517
     */

    private static Map<String, Set<String>> tableMap = new HashMap<>();

    private static Set<String> patientSet = new TreeSet<>();

    private static Map<String, Integer> patientIndexMap;

    private static Map<String, LongAdder> tableIdCountMap = new HashMap<>();

    private static Set<String> numericTableIdSet = new HashSet<>();

    private static Set<String> superSparseTableIdSet = new HashSet<>();

    private static Set<String> numericDenseTableIdSet = new TreeSet<>();

    private static Map<String, Integer> numericDenseTableIdIndexMap;

    private static Map<String, LongAdder> numericDenseTableIdCountMap = new HashMap<>();

    private static Map<String, DoubleAdder> numericDenseTableIdAvgMap = new HashMap<>();

    private static Map<String, String> patientResultMap = new HashMap<>();

    private static double[][] result;

    private static void readIntoTableMap(String fileName) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        bufferedReader.readLine();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split("\\$");
            String vid = split[0];
            patientSet.add(vid);
            String tableId = split[1];
            tableIdCountMap.computeIfAbsent(tableId, k -> new LongAdder()).increment();
            String results = split.length >= 3 ? split[2] : "";
            results = replaceResults(results);
            tableMap.computeIfAbsent(tableId, k -> new HashSet<>())
                    .add(results);
        }
        AtomicInteger longAdder = new AtomicInteger();
        patientIndexMap = patientSet.stream().collect(Collectors.toMap(
                Function.identity(),
                s -> longAdder.getAndIncrement()
        ));
        bufferedReader.close();
    }

    static String replaceResults(String results) {
        results = results.replaceAll("０", "0");
        results = results.replaceAll("１", "1");
        results = results.replaceAll("２", "2");
        results = results.replaceAll("３", "3");
        results = results.replaceAll("４", "4");
        results = results.replaceAll("５", "5");
        results = results.replaceAll("６", "6");
        results = results.replaceAll("７", "7");
        results = results.replaceAll("８", "8");
        results = results.replaceAll("９", "9");
        results = results.replaceAll("阴性", "0");
        results = results.replaceAll("阳性", "1");
//            results = results.replaceAll("无", "0");
//            results = results.replaceAll("[0-9]+级", "");
        return results;
    }

    private static void extractNumericTableIdSet() {
        tableMap.forEach((tableId, resultSet) -> {
            if (isNumeric(resultSet)) {
                numericTableIdSet.add(tableId);
            }
        });
    }

    private static void extractSuperSparseTableIdSet() {
        superSparseTableIdSet = tableIdCountMap.entrySet().stream().filter(e -> e.getValue().intValue() <= 1)
                .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    private static void extractNumericDenseTableIdSet() {
        numericDenseTableIdSet = new HashSet<>(numericTableIdSet);
        numericDenseTableIdSet.removeAll(superSparseTableIdSet);
    }

    private static boolean isNumeric(Set<String> strings) {
        return strings.stream().allMatch(s -> {
            try {
                if (s.length() != 0) {
                    Double.valueOf(s);
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    private static void readIntoNumericDenseTableIdAvgMap(String fileName) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        bufferedReader.readLine();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split("\\$");
            String tableId = split[1];
            if (!numericDenseTableIdSet.contains(tableId)) {
                continue;
            }
            String results = split.length >= 3 ? split[2] : "";
            results = replaceResults(results);
            if (results.isEmpty()) {
                continue;
            }
            numericDenseTableIdAvgMap.computeIfAbsent(tableId, k -> new DoubleAdder()).add(Double.valueOf(results));
            numericDenseTableIdCountMap.computeIfAbsent(tableId, k -> new LongAdder()).increment();
        }
        bufferedReader.close();
        numericDenseTableIdAvgMap.replaceAll((tableId, avg) -> {
            double avgValue = avg.sumThenReset() / numericDenseTableIdCountMap.get(tableId).intValue();
            avg.add(avgValue);
            return avg;
        });
        numericDenseTableIdSet.removeIf(k -> !numericDenseTableIdAvgMap.containsKey(k));
        numericDenseTableIdCountMap.clear();
        AtomicInteger atomicInteger = new AtomicInteger();
        numericDenseTableIdIndexMap = numericDenseTableIdSet.stream().collect(Collectors.toMap(
                Function.identity(),
                s -> atomicInteger.getAndIncrement()
        ));
        System.out.println(numericDenseTableIdAvgMap.size());
        System.out.println(numericDenseTableIdSet.size());
        System.out.println(numericDenseTableIdAvgMap.values().stream().filter(doubleAdder -> doubleAdder.doubleValue() == 0.0).count());
    }

    private static void initResult() {
        result = new double[patientSet.size()][numericDenseTableIdSet.size()];
        for (int i = 0; i < result.length; ++i) {
            for (int j = 0; j < result[0].length; ++j) {
                result[i][j] = -1;
            }
        }
    }

    private static void readIntoPatientResultMap(String fileName) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        bufferedReader.readLine();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String vid = line.split(",")[0];
            String result = line.substring(line.indexOf(',') + 1);
            patientResultMap.put(vid, result);
        }
        bufferedReader.close();
    }

    private static void writeIntoOut(String inputFileName, String outputFileName) throws IOException {
        initResult();
        patientSet.clear();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileName));
        bufferedReader.readLine();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split("\\$");
            String vid = split[0];
            String tableId = split[1];
            String results = split.length >= 3 ? split[2] : "";
            results = replaceResults(results);
            if (numericDenseTableIdSet.contains(tableId)) {
                result[patientIndexMap.get(vid)][numericDenseTableIdIndexMap.get(tableId)] =
                        results.isEmpty() ?
                                numericDenseTableIdAvgMap.getOrDefault(tableId, new DoubleAdder()).doubleValue() :
                                Double.valueOf(results);
            }
        }
        bufferedReader.close();
        numericDenseTableIdAvgMap.forEach((tableId, avg) -> {
            Integer tableIdIndex = numericDenseTableIdIndexMap.get(tableId);
            for (int i = 0; i < result.length; ++i) {
                if (result[i][tableIdIndex] == -1) {
                    result[i][tableIdIndex] = avg.doubleValue();
                }
            }
        });
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
        StringBuilder lineBuilder = new StringBuilder();
        patientIndexMap.forEach((vid, index) -> {
            if (!patientResultMap.containsKey(vid)) {
                return;
            }
            for (double item : result[index]) {
                lineBuilder.append(String.format("%.3f", item)).append(",");
            }
            lineBuilder.append(patientResultMap.get(vid));
            try {
                bufferedWriter.write(lineBuilder.toString());
                bufferedWriter.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            lineBuilder.delete(0, lineBuilder.length());
        });
        bufferedWriter.close();
    }

    public static void main(String[] args) throws IOException {
        String fileName = "C:\\Users\\chamtail\\Desktop\\skypound_data\\meinian_round1_data.txt";
        DataReader.readIntoTableMap(fileName);
        extractNumericTableIdSet();
        tableMap.clear();
        System.out.println(numericTableIdSet.size());
        System.out.println(patientSet.size());
        extractSuperSparseTableIdSet();
        System.out.println(superSparseTableIdSet.size());
        extractNumericDenseTableIdSet();
        numericTableIdSet.clear();
        superSparseTableIdSet.clear();
        System.out.println(numericDenseTableIdSet.size());
        readIntoNumericDenseTableIdAvgMap(fileName);
        System.out.println(JSON.toJSONString(numericDenseTableIdAvgMap));
        readIntoPatientResultMap("C:\\Users\\chamtail\\Desktop\\skypound_data\\meinian_round1_train_20180408.csv");
        writeIntoOut(fileName, "C:\\Users\\chamtail\\Desktop\\skypound_data\\tp_data_out");
    }
}
