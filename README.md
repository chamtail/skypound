package vip.theo.skyfound;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataReader {
/**
*最简陋版本		1055
*阴性=0 阳性=1	1507
*无=0			1517
*/

    private static Map<String, Set<String>> tableMap = new HashMap<>();

    private static Set<String> numericTableSet = new HashSet<>();

    private static void readIntoTableMap(String fileName) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        bufferedReader.readLine();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split("\\$");
            String tableId = split[1];
            String results = split.length >= 3 ? split[2] : "";
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
            tableMap.computeIfAbsent(tableId, k -> new HashSet<>())
                    .add(results);
        }
        bufferedReader.close();
    }

    private static void writeToFile(String fileName, String targetFileName) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        bufferedReader.readLine();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(targetFileName));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split("\\$");
            String tableId = split[1];
            if (numericTableSet.contains(tableId)) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
        }
        bufferedReader.close();
        bufferedWriter.close();
    }

    private static void extractNumericTableSet() {
        tableMap.forEach((tableId, resultSet) -> {
            if (isNumeric(resultSet)) {
                numericTableSet.add(tableId);
            }
        });
    }

    private static boolean isNumeric(Set<String> strings) {
        return strings.stream().allMatch(s -> {
            try {
                if (s.length() != 0) {
                    Double.valueOf(s);
                }
                return true;
            } catch (Exception e) {
//                System.out.println(s + " " + s.length());
                return false;
            }
        });
    }

    public static void main(String[] args) throws IOException {
        String fileName1 = "C:\\Users\\chamtail\\Desktop\\meinian_round1_data_part1_20180408.txt";
        String fileName2 = "C:\\Users\\chamtail\\Desktop\\meinian_round1_data_part2_20180408.txt";
        DataReader.readIntoTableMap(fileName1);
        DataReader.readIntoTableMap(fileName2);
        extractNumericTableSet();
        System.out.println(numericTableSet.size());
        writeToFile(fileName1, "1.out");
        writeToFile(fileName2, "2.out");
    }
}
