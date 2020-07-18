public class Main {
    public static void main(String[] args) {
        // 服务器监听端口号
        int port = 8848;
        try {
            new HttpServer(port).start();
        } catch (Exception e) {
            System.out.println("task start failed");
            e.printStackTrace();
        }
    }
}
