import java.util.ArrayList;

public class GroupInfo {
    private int num;
    private ArrayList<Integer> splitPoints;

    public GroupInfo(int num, ArrayList<Integer> splitPoints) {
        this.num = num;
        this.splitPoints = splitPoints;
    }
    public int getNum() {
        return num;
    }

    public ArrayList<Integer> getSplitPoints() {
        return splitPoints;
    }
}
