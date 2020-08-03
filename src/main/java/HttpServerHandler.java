import com.sun.org.apache.xpath.internal.objects.XObject;
import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.ServiceTarget;
import com.tencent.tdw.security.authentication.client.SecureClient;
import com.tencent.tdw.security.authentication.client.SecureClientFactory;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.apache.calcite.avatica.AvaticaStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServerHandler extends ChannelInboundHandlerAdapter {//入栈（channel）时间适配器

    private final static Logger logger = LoggerFactory.getLogger(EchoServerHandler.class);
    private AtomicBoolean _running = new AtomicBoolean(true);
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(8,16,2000,
            TimeUnit.MILLISECONDS,new LinkedBlockingDeque<>());
    private AsciiString contentType = HttpHeaderValues.TEXT_PLAIN;
    private HttpResponse response;

    static {
        try {
            Class.forName("com.tencent.supersql.jdbc.SuperSqlDriver");//独特的驱动
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String generateAuth() {
        try {
//Auth code
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //supersql查询结果输出
    private void SaveDS(BufferedWriter stdout, ResultSet rs) throws SQLException, IOException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnNum = rsmd.getColumnCount();

        int i;
        for(i = 1; i <= columnNum; ++i) {
            stdout.write(rsmd.getColumnName(i) + "\t");
        }

        stdout.newLine();

        while(rs.next()) {
            for(i = 1; i <= columnNum; ++i) {
                switch(rsmd.getColumnType(i)) {
                    case -6:
                        stdout.write(rs.getByte(i) + "\t");
                        break;
                    case -5:
                        stdout.write(rs.getLong(i) + "\t");
                        break;
                    case 3:
                        stdout.write(rs.getBigDecimal(i) + "\t");
                        break;
                    case 4:
                        stdout.write(rs.getInt(i) + "\t");
                        break;
                    case 5:
                        stdout.write(rs.getShort(i) + "\t");
                        break;
                    case 6:
                    case 7:
                        stdout.write(rs.getFloat(i) + "\t");
                        break;
                    case 8:
                        stdout.write(rs.getDouble(i) + "\t");
                        break;
                    case 16:
                        stdout.write(rs.getBoolean(i) + "\t");
                        break;
                    case 91:
                        stdout.write(rs.getDate(i) + "\t");
                        break;
                    case 92:
                        stdout.write(rs.getTime(i) + "\t");
                        break;
                    case 93:
                        stdout.write(rs.getTimestamp(i) + "\t");
                        break;
                    default:
                        stdout.write(rs.getString(i) + "\t");
                }
            }
            stdout.newLine();
//                    System.out.println();
        }

        stdout.newLine();
        stdout.flush();
        rs.close();
    }

    private void processQueryString(HttpRequest req, String uri){

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if(msg instanceof HttpRequest){
            HttpRequest request = (HttpRequest)msg;
            String uri = request.uri();

            request.method();


            System.out.println("class:" + msg.getClass().getName());
/*            if(in!=null&&!in.equals("")){
                String[] input = in.split("//");
                System.out.println();
                System.out.println(input[0]+" execute "+input[1]);
                System.out.println();
                String name = input[1].split("\\.")[0];
                ctx.write(input[0]+input[1]+"收到！$_");
//                    System.out.println(input[1]);

                if ( !Thread.currentThread().isInterrupted()){
                    executor.execute(new multiCal(ctx,name,input[2]));
                    int queueSize = executor.getQueue().size();
                    if (queueSize > 200) {
                        logger.info("Job queue size >200, fetch too many jobs, wait for a moment.");
                        Thread.sleep(10000);
                    }
                }

            }*/
        } else {
            ReferenceCountUtil.release(msg);//
        }



        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer("test".getBytes())); // 2

        HttpHeaders heads = response.headers();
        heads.add(HttpHeaderNames.CONTENT_TYPE, contentType + "; charset=UTF-8");
        heads.add(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes()); // 3
        heads.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);//http keep-Alive




        ctx.write(response);
    }

    private class multiCal implements Runnable{

        private String sql;
        private String name;
        private ChannelHandlerContext ctx;
        public multiCal(ChannelHandlerContext ctx,String name,String sql){
            this.ctx = ctx;
            this.name = name;
            this.sql = sql;
        }
        @Override
        public void run() {

//          String supersqlServer = args[1];
            String url = "jdbc:avatica:remote:url=http://ss-qe-supersql-pcg:8081";
//            String url = "jdbc:mysql://127.0.0.1:3306/db1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC" ;
//            String username = "root";
//            String password = "123456";
            Connection conn = null;
            Statement st = null;
            ResultSet rs = null;
            PrintStream stdout = null;
            try {
//                String sql = new String(Files.readAllBytes(Paths.get(fileName)));//文件读取sql
//                System.out.println(sql);//显示
                try {
                    Driver driver = DriverManager.getDriver(url);//jdbc
                    Properties p = new Properties();
                    //                p.put(username,password);
                    p.put("rawAuth", generateAuth());//TDW安全参数
                    conn = driver.connect(url,p);
                    //                conn = DriverManager.getConnection(url,username,password);
                    st = conn.createStatement();
                    AvaticaStatement as = (AvaticaStatement) st;

                    File file = new File("/data/tools/supersql_autotest/autotest_jar/out/"+
                            new SimpleDateFormat("yyyy-MM-dd").format(new Date())+"/"+name+"/"+as.getId()+"_"+as.getConnection().id+"_"+"Querystderr.txt");
                    if(!file.getParentFile().exists()){
                        System.out.println("创建文件"+file.getParentFile().mkdirs());
                    }
                    stdout = new PrintStream(file);
                    // get query log for statement asynchronously
                    QueryLogThread queryLogThread = new QueryLogThread(as,name);
                    queryLogThread.start();

                    // query start
                    rs = st.executeQuery(sql);
//                    SaveDS(stdout,rs);
                    System.out.println("Query 运行结束!");
                } catch (Exception throwables) {
                    throwables.printStackTrace(stdout);
//                    stdout.write(throwables.toString()+"\n");
                }

//                ctx.write(rs.toString());
//                stdout.write();
//                printResultSet(rs);

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (rs !=null){
                    try{
                        rs.close();
                    } catch (Exception ignored) {}
                }
                if(st !=null){
                    try{
                        st.close();
                    } catch (Exception ignored) {}
                }
                if(conn !=null){
                    try{
                        conn.close();
                    } catch (Exception ignored) {}
                }
            }
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Server active ");
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Server close ");
        super.channelInactive(ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {

    }

}
