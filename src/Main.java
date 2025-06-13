import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws Exception {
        int N = 5;
        if (args.length < 1) {
            System.err.println("Usage: java Main <input_file_path>");
            System.exit(1);
        }

        String filePath = args[0];
        ArrayList<String> tasks = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                tasks.add(line.trim());
            }
        }

        MasterNode master = new MasterNode(12000, N);
        master.start(tasks);

        // Wait for messages to be delivered
        Thread.sleep(2000);
        System.out.println(master.toString());

        return;
    }
}