import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterNode {
    private int N;
    private int port;
    private ServerHandler handler;
    private WorkerNode[] mappers;
    private AtomicInteger[] status;
    private boolean allTasksDispatched = false;
    private enum Phase { MAPPING1, REDUCING1, MAPPING2, REDUCING2 }
    private Phase currentPhase = Phase.MAPPING1;
    private int globalMax = -Integer.MAX_VALUE;
    private int globalMin = Integer.MAX_VALUE;
    private int totalSize = 0;

    public MasterNode(int port, int N) throws Exception {
        this.port = port;
        this.N = N;
        this.handler = new ServerHandler();
        this.mappers = new WorkerNode[N];
        this.status = new AtomicInteger[N];
        for (int i = 0; i < N; i++) {
            status[i] = new AtomicInteger(0);
        }
        initializeMappers();
    }

    public void start(ArrayList<String> tasks) throws Exception {
        handler.startServer(port, this::processMasterMessage);
        distributeTasks(tasks);
    }

    private void initializeMappers() throws Exception {
        for (int i = 0; i < N; i++) {
            mappers[i] = new WorkerNode(i, N);
            int nodePort = this.port + i + 1;
            mappers[i].startServer(nodePort);
        }
    }

    private void processMasterMessage(Message msg){
        try{
            if (msg.getMsgType() == -1) {
                // MessageType: WorkerCurrentTaskFinished
                status[(int) msg.getPayload()].decrementAndGet();
                if (checkStatus()) {
                    if(currentPhase == Phase.MAPPING1 && allTasksDispatched){
                        System.out.println("All nodes finished mapping.");
                        resetStatus(1); // the following task (reduce1) is strictly individual
                        currentPhase = Phase.REDUCING1;
                        Message startReduce1 = new Message(2);
                        broadcastMessage(startReduce1);
                    }
                    else if(currentPhase == Phase.REDUCING1){
                        System.out.println("All nodes finished reducing.");
                        resetStatus(1); // the following task (local sort) is strictly individual
                        currentPhase = Phase.MAPPING2;
                        Message startLocalSort = new Message(3);
                        broadcastMessage(startLocalSort);
                    }
                    else if(currentPhase == Phase.MAPPING2){
                        System.out.println("All nodes finished redistributing.");
                    }
                }
            }
            else if (msg.getMsgType() == -2) {
                // Message Type: WorkerCurrentLocalSortingFinished
                LocalSortResult res = (LocalSortResult) msg.getPayload();
                status[res.getId()].decrementAndGet();
                globalMax = Math.max(res.getMax(), globalMax);
                globalMin = Math.min(res.getMin(), globalMin);
                totalSize += res.getSize();
                if (checkStatus() && currentPhase == Phase.MAPPING2) {
                    System.out.println("All nodes finished local sorting.");
                    ArrayList<Integer> groupInfo = group(globalMin, globalMax, N);
                    resetStatus(1);
                    Message startMapping2  = new Message(4, groupInfo);
                    broadcastMessage(startMapping2);
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void distributeTasks(ArrayList<String> tasks) throws IOException {
        int nodePtr = 0;
        for(String task : tasks) {
            Message msg = new Message(0, task);
            handler.sendMessage("localhost", port + nodePtr + 1, msg);
            status[nodePtr].incrementAndGet();;
            nodePtr++;
            nodePtr %= this.N;
        }
        allTasksDispatched = true;
    }

    private ArrayList<Integer> group(int min, int max, int workerSize){
        ArrayList<Integer> splitPoints = new ArrayList<>();
        int range = max - min + 1;
        int step = range / workerSize;
        int remainder = range % workerSize;

        int current = min;
        for (int i = 0; i < workerSize; i++) {
            int width = step + (i < remainder ? 1 : 0);  // distribute remainder evenly
            current += width - 1;
            splitPoints.add(current);
            current++; // move to next bucket
        }
        return splitPoints;
    }

    private void resetStatus(int value) {
        for (AtomicInteger i : status) {
            i.set(value);
        }
    }

    private boolean checkStatus(){
        for (AtomicInteger i : status) {
            if (i.get() != 0) return false;
        }
        return true;
    }

    private void broadcastMessage(Message msg) throws IOException {
        for (int i = 0; i < mappers.length; i++) {
            handler.sendMessage("localhost", port+i+1, msg);
        }
    }
}
