import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class WorkerNode {
    private int id;
    private int N;
    private int port;
    private ServerHandler handler;
    private ArrayList<WordPair> words = new ArrayList<>();
    private Map<String, Integer> wordCounts = new ConcurrentHashMap<>();

    public WorkerNode(int id, int N) {
        this.id = id;
        this.N = N;
        this.handler = new ServerHandler();
    }

    public void startServer(int port) throws Exception {
        this.port = port;
        handler.startServer(port, this::handleMessage);
    }

    private void handleMessage(Message msg) {

        try{
            if (msg.getMsgType() == 0 && msg.getPayload() instanceof String) {
                // MessageType: MasterTaskDistribution
                countAndShuffle(msg);
            }
            if (msg.getMsgType() == 1 && msg.getPayload() instanceof WordPair) {
                // MessageType: PeerWordPair1
                this.saveWordPairs(msg, 1);
            }
            if (msg.getMsgType() == 2) {
                // MessageType: MasterInstructionToReduce
                this.reduce2Counts();
            }
            if (msg.getMsgType() == 3) {
                // MessageType: MasterInstructionToLocalSort
                this.sortWordCounts(true, true);
            }
            if  (msg.getMsgType() == 4){
                // MessageType: MasterInstructionToShuffle2
                this.redistribute(msg);
            }
            if (msg.getMsgType() == 5){
                // MessageType: PeerWordPair
                this.saveWordPairs(msg, 2);
            }
            if (msg.getMsgType() == 6){
                // MessageType: MasterInstructionToReduce2
                this.sortWordCounts(true, false);
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        // Extend for other msgTypes
    }

    private void countAndShuffle(Message msg) throws IOException {
        // This method takes one Message containing a String at a time
        // splits into words, generates WordPairs with count=1
        // sends the generated WordPairs according to hash function
        // and reports to master node once it has finished
        String text = msg.getPayload().toString();
        for (String word : text.split("\\s+")) {
            WordPair wp = new WordPair(word, 1);
            Message wpMsg = new Message(1, wp);
            int destId = Math.abs(word.hashCode()) % N;
            handler.sendMessage("localhost", port-id + destId, wpMsg);
            // System.out.println("Node " + id + " transmitted " + word + " to node " + destId);
        }
        Message finiMsg = new Message(-1, this.id);
        handler.sendMessage("localhost", port-id-1, finiMsg);
        System.out.println("Node " + id + "finished shuffling ");
    }

    private void saveWordPairs(Message msg, int stage) throws IOException {
        // This method takes one Message containing a WordPair at a time
        // and adds the WordPair into its own list
        WordPair wp = (WordPair)msg.getPayload();
        if(stage == 1){
            this.words.add(wp);
        }
        else if(stage == 2){
            this.wordCounts.put(wp.word, wp.count);
        }
    }

    private void reduce2Counts() throws IOException {
        // This method iterates over LOCAL WordPair list
        // to count the word frequency
        // and reports to master node once it has finished
        for(WordPair wp : words){
            wordCounts.put(wp.word, wordCounts.getOrDefault(wp.word, 0) + wp.count);
            // System.out.println("Node " + id + " has " + wp.word + wordCounts.get(wp.word));
        }
        Message finished = new Message(-1, this.id);
        handler.sendMessage("localhost", port-id-1, finished);
        System.out.println("Node " + id + "finished reducing ");
    }

    private void sortWordCounts(boolean ascending, boolean byCounts) throws IOException {
        // This method sort LOCAL wordCounts map
        // and reports to master node once it has finished
        // This method is updated to be able to sort according to words for re-using
        Comparator<Map.Entry<String, Integer>> comparator = byCounts
                ? Map.Entry.comparingByValue()
                : Map.Entry.comparingByKey();

        if (!ascending) {
            comparator = comparator.reversed();
        }

        Map<String, Integer> sortedMap = wordCounts.entrySet()
                .stream()
                .sorted(comparator)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, // merge function
                        LinkedHashMap::new // keeps insertion order
                ));

        wordCounts = sortedMap;
        printCounts();

        // Only compute max, min, size if sorting by count
        int max = -1, min = -1;
        if (byCounts && !wordCounts.isEmpty()) {
            max = wordCounts.entrySet().stream()
                    .max(Map.Entry.comparingByValue()).orElseThrow().getValue();
            min = wordCounts.entrySet().stream()
                    .min(Map.Entry.comparingByValue()).orElseThrow().getValue();
            int size = wordCounts.size();
            LocalSortResult res = new LocalSortResult(max, min, size, id);
            Message localSort = new Message(-2, res);
            handler.sendMessage("localhost", port-id-1, localSort);
        }

    }

    private void redistribute(Message msg) throws IOException {
        ArrayList<Integer> splits = (ArrayList<Integer>) msg.getPayload();
        Map<String, Integer> myNewWordCounts = new ConcurrentHashMap<>();

        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            String word = entry.getKey();
            int count = entry.getValue();

            // Determine which worker this word-count should go to
            int destId = 0;
            for (int i = 0; i < splits.size(); i++) {
                if (count <= splits.get(i)) {
                    destId = i;
                    break;
                }
            }

            if (destId == this.id) {
                myNewWordCounts.put(word, count);
            } else {
                WordPair wp = new WordPair(word, count);
                Message wpMsg = new Message(5, wp);
                handler.sendMessage("localhost", port-id + destId, wpMsg);
            }
        }
        this.wordCounts = myNewWordCounts;
        Message finiMsg = new Message(-1, this.id);
        handler.sendMessage("localhost", port-id-1, finiMsg);
    }

    private void printCounts() {
        wordCounts.forEach((k, v) -> System.out.println("Node " + id + " " + k + ": " + v));
    }
}
