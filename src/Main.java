import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws Exception {
        int N = 3;

        MasterNode master = new MasterNode(12000, 3);
        ArrayList<String> tasks = new ArrayList<>();

        tasks.add(0,"car car hello cat");
        tasks.add(1, "dog cat cat hi");
        tasks.add(2, "hi hello car car");
        tasks.add(3, "hi hi hi hello car");

        master.start(tasks);

        // Wait for messages to be delivered
        Thread.sleep(2000);
        System.out.println(master.toString());

        return;
    }
}