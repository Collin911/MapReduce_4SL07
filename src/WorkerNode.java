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
                this.saveWordPairs(msg);
            }
            if (msg.getMsgType() == 2) {
                // MessageType: MasterInstructionToReduce
                this.reduce2Counts();
            }
            if (msg.getMsgType() == 3) {
                this.sortByTimes(false);
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        // Extend for other msgTypes
    }

    private void countAndShuffle(Message msg) throws IOException {
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

    private void saveWordPairs(Message msg) throws IOException {
        WordPair wp = (WordPair)msg.getPayload();
        this.words.add(wp);
    }

    private void reduce2Counts() throws IOException {
        for(WordPair wp : words){
            wordCounts.put(wp.word, wordCounts.getOrDefault(wp.word, 0) + wp.count);
            // System.out.println("Node " + id + " has " + wp.word + wordCounts.get(wp.word));
        }
        Message finished = new Message(-1, this.id);
        handler.sendMessage("localhost", port-id-1, finished);
        System.out.println("Node " + id + "finished reducing ");
    }

    /*private void sortByTimes(){
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(this.wordCounts.entrySet());
        entryList.sort((e1, e2) -> e1.getValue().compareTo(e2.getValue()));

        Map<String, Integer> sortedMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, Integer> entry : entryList) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        this.wordCounts = sortedMap;
        printCounts();
        Message localSort = new Message();
    }*/

    private void sortByTimes(boolean ascending) {
        Comparator<Map.Entry<String, Integer>> comparator =
                Map.Entry.comparingByValue();

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
        int max = wordCounts.entrySet().stream().max(Map.Entry.comparingByValue()).orElse(null).getValue();
        int min = wordCounts.entrySet().stream().min(Map.Entry.comparingByValue()).orElse(null).getValue();
        int size = wordCounts.size();
        LocalSortResult res = new LocalSortResult(max, min, size, id);
        Message localSort = new Message(-2, res);
    }

    private void printCounts() {
        wordCounts.forEach((k, v) -> System.out.println("Node " + id + " " + k + ": " + v));
    }
}
