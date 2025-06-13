import java.io.Serializable;

public class LocalSortResult implements Serializable {
    private int max;
    private int min;
    private int size;
    private int id;

    public LocalSortResult(int max, int min, int size, int id) {
        this.max = max;
        this.min = min;
        this.size = size;
        this.id = id;
    }
    public int getMax() {
        return max;
    }
    public int getMin() {
        return min;
    }
    public int getSize() {
        return size;
    }
    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "id: " + id + "size: " + size + "max: " + max + "min: " + min;
    }
}

