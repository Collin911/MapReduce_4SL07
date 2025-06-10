import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
                countAndShuffle((String) msg.getPayload());
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
                this.sortByTimes();
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        // Extend for other msgTypes
    }

    private void countAndShuffle(String text) throws IOException {
        for (String word : text.split("\\s+")) {
            WordPair wp = new WordPair(word, 1);
            Message msg = new Message(1, wp);
            int destId = Math.abs(word.hashCode()) % N;
            handler.sendMessage("localhost", port-id-1 + destId, msg);
        }
        Message finished = new Message(-1, this.id);
        handler.sendMessage("localhost", port-id-1, finished);
        System.out.println("Node" + id + "finished shuffling ");
    }

    private void saveWordPairs(Message msg) throws IOException {
        WordPair wp = (WordPair)msg.getPayload();
        this.words.add(wp);
    }

    private void reduce2Counts() throws IOException {
        for(WordPair wp : words){
            wordCounts.put(wp.word, wordCounts.getOrDefault(wp.word, 0) + wp.count);
        }
        Message finished = new Message(-1, this.id);
        handler.sendMessage("localhost", port-id-1, finished);
        System.out.println("Node" + id + "finished reducing ");
    }

    private void sortByTimes(){
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(this.wordCounts.entrySet());
        entryList.sort((e1, e2) -> e1.getValue().compareTo(e2.getValue()));

        Map<String, Integer> sortedMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, Integer> entry : entryList) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        this.wordCounts = sortedMap;
        printCounts();
    }

    private void printCounts() {
        wordCounts.forEach((k, v) -> System.out.println(k + ": " + v));
    }
}