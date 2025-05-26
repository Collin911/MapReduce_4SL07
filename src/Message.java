import java.io.Serializable;

public class Message implements Serializable {
    public int msgType;
    public Object payload;

    public Message(){
        this.msgType = -1;
        this.payload = null;
        // This is a null message which should NOT be used
    }

    public Message(int msgType, Object payload) {
        this.msgType = msgType;
        this.payload = payload;
    }
}