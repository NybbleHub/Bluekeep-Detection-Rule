package com.bluekeep.rule.markovchain;

import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class MarkovChainTraining {

    private Map<Character, Integer> alphabetMap = new HashMap<Character, Integer>();
    private Double[][] probabilityMatrix = null;
    private List<String> usernameTrainingList = new ArrayList<String>();
    private String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
    private Integer MIN_COUNT_VAL = 10;
    private Double threshold = 0D;
    private List<String> notRandomUsername = new ArrayList<String>();
    private List<String> randomUsername = new ArrayList<String>();

    public void trainModel() throws FileNotFoundException {
        initAlphabetMap();

        // Import username from CSV file
        Scanner usernameFile = new Scanner(new BufferedReader(new FileReader(new File("./src/main/resources/MarkovChain-Full-Cleaned.csv"))));

        while(usernameFile.hasNextLine()) {
            String line = usernameFile.nextLine();
            usernameTrainingList.add(line);
        }

        // Create the 2D Alphabet Matrix
        Integer[][] alphabetMatrix = getAlphabetMatrix(usernameTrainingList);

        // Create a 2D Probabilty Matrix following counts in the Alphabet Matrix
        probabilityMatrix = getProbabilityMatrix(alphabetMatrix);

        notRandomUsername.add("jsmith");
        notRandomUsername.add("mgarcia");
        notRandomUsername.add("mrodriguez");
        notRandomUsername.add("mhernandez");
        notRandomUsername.add("jjohnson");
        notRandomUsername.add("knguyen");
        notRandomUsername.add("dgriffith");
        notRandomUsername.add("tpatrick");
        notRandomUsername.add("aanderson");

        randomUsername.add("iaDz2xEdjyPb0");
        randomUsername.add("laPBnNN0RLPbF");
        randomUsername.add("4CFt9jfY9jwoL");
        randomUsername.add("Pb0JAsWMlGvFF");
        randomUsername.add("aRoS19tTFvpUQ");
        randomUsername.add("iVb7XTDi2fODI");
        randomUsername.add("2m0Efr0Pve0N6");
        randomUsername.add("mVxJMBtHNNvCf");

        List<Double> goodProbability = getAvgTransitionProbability(notRandomUsername, probabilityMatrix);
        List<Double> badProbability = getAvgTransitionProbability(randomUsername, probabilityMatrix);

        // Return the smallest value from goodProbability Double List.
        Double minGood = Collections.min(goodProbability);
        // Return the greatest value from badProbability Double List.
        Double maxBad = Collections.max(badProbability);

        if (minGood <= maxBad) {
            throw new AssertionError("Cannot create a threshold");
        }
        // Return (minGood + maxBad) / 2 value (Average)
        threshold = getThreshold(minGood, maxBad);
    }


    public boolean isRandom(String username) {
        // Compare TransitionProbability for String Username to threshold value.
        // If TransitionProbability is Greater than threshold => Return false => String may not have been randomly generated
        // If TransitionProbability is Lower than threshold => Return true => String may have been randomly generated
        return !(getAvgTransitionProbability(username, probabilityMatrix) > threshold);
    }


    private void initAlphabetMap() {
        char[] alphabetChars = alphabet.toCharArray();
        for (Integer x = 0; x < alphabetChars.length; x++) {
            alphabetMap.put(alphabetChars[x], x);
        }
    }

    private Integer[][] getAlphabetMatrix(List<String> usernameTrainingList){
        // Create 2D Alphabet Matrix where # of column equal alphabet length and # of lines equal alphabet length
        Integer[][] counts = createArray(alphabet.length());

        for(String line: usernameTrainingList) {
            // Get nGram for each Username in the list from usernameFile
            List<String> nGram = getNgram(2, line);

            for (String tuple : nGram) {
                // For each tuple (Can be biGram, TriGram, and so on...) in nGram String list
                // Get first Char in tuple and increase count by one at Char position in 2D Matrix
                // Get second Char in tuple and increase count by one at Char position in 2D Matrix
                counts[alphabetMap.get(tuple.charAt(0))][alphabetMap.get(tuple.charAt(1))]++;
            }
        }
        return counts;
    }

    private Integer[][] createArray(Integer length) {
        Integer[][] counts = new Integer[length][length];

        // Initialize Matrix and fill this one with MIN_COUNT_VAL
        for(Integer x = 0; x < counts.length; x++) {
            Arrays.fill(counts[x], MIN_COUNT_VAL);
        }
        return counts;
    }

    private List<String> getNgram(Integer n, String line) {
        List<String> nGram = new ArrayList<String>();

        for(Integer x = 0; x < line.length() - n +1; x++) {
            nGram.add(line.substring(x, x + n));
        }
        // Return a list of all nGram for a Username
        // Example for Username johndoe => jo, oh, hn, nd, do, oe
        return nGram;
    }

    private Double[][] getProbabilityMatrix(Integer[][] alphabetMatrix) {
        Integer alphabetLength = alphabet.length();
        // Create 2D Alphabet Matrix where # of column equal alphabet length and # of lines equal alphabet length.
        Double[][] probabilityMatrix = new Double[alphabetLength][alphabetLength];

        for(Integer x = 0; x < alphabetMatrix.length; x++) {
            // For each column (x) get sum of all value in lines (y)
            Double sum = getSum(alphabetMatrix[x]);
            for(Integer y = 0; y < alphabetMatrix[x].length; y++) {
                // Fill Probability Matrix with Logarithm of AlphabetBet Matrix value for Char in position [x][y]
                // Divided by $sum previously calculated.
                probabilityMatrix[x][y] = Math.log(alphabetMatrix[x][y]/sum);
            }
        }
        return probabilityMatrix;
    }

    private Double getSum(Integer[] array) {
        Double sum = 0D;
        for(Integer x = 0; x < array.length; x++) {
            sum += array[x];
        }
        return sum;
    }

    private List<Double> getAvgTransitionProbability(List<String> lines, Double[][] probabilityMatrix) {
        List<Double> result = new ArrayList<Double>();

        for(String line : lines) {
            result.add(getAvgTransitionProbability(line, probabilityMatrix));
        }
        return result;
    }

    private Double getAvgTransitionProbability(String line, Double[][] probabilityMatrix) {
        Double logProbability = 0D;
        Integer transitionCount = 0;

        // Get nGram for Username in String line
        List<String> nGram = getNgram(2, line);

        for(String tuple : nGram) {
            // For each tuple (Can be biGram, TriGram, and so on...) in nGram String list
            // Get first Char in tuple and add probability value from Probability Matrix to $logProbability
            // Get second Char in tuple and add probability value from Probability Matrix to $logProbability
            logProbability += probabilityMatrix[alphabetMap.get(tuple.charAt(0))][alphabetMap.get(tuple.charAt(1))];
            transitionCount++;
        }
        // Return Euler's number e raised to the power of ($logProbability value divided by $transitionCount value
        // or 1 if transitionCount is smaller than 1.
        return Math.exp(logProbability / Math.max(transitionCount, 1));
    }

    protected Double getThreshold(Double minGood, Double maxBad) {
        return (minGood + maxBad) / 2;
    }
}
