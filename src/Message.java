import java.io.Serializable;

public class Message implements Serializable {
    private int msgType;
    private Object payload;

    public Message(){
        this.msgType = Integer.MIN_VALUE;
        this.payload = null;
        // This is a null message which should NOT be used
    }

    public Message(int msgType){
        this.msgType = msgType;
        this.payload = null;
    }

    public Message(int msgType, Object payload) {
        this.msgType = msgType;
        this.payload = payload;
    }

    public int getMsgType() {
        return msgType;
    }

    public Object getPayload() {
        return payload;
    }
}
