import com.sun.org.apache.xerces.internal.parsers.CachingParserPool;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.remote.Service;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

class QueryLogThread extends Thread {

//    private BufferedWriter stdout ;
    private PrintStream stderr = null;
    private String name;
//    private PrintStream psout = null;
//    private PrintStream pserr = null;
    private AvaticaStatement avaticaStatement;

    public QueryLogThread(AvaticaStatement avaticaStatement,String name) {
        this.avaticaStatement = avaticaStatement;
        this.name = name;
//        this.stdout = stdout;
    }


    @Override
    public void run() {
        try {

            // 获取connctionId、statementId
            String avaticaConnectionId = avaticaStatement.getConnection().id;
            int avaticaStatementId = avaticaStatement.getId();

            File file = new File("/data/tools/supersql_autotest/autotest_jar/out/"+
                    new SimpleDateFormat("yyyy-MM-dd").format(new Date())+"/"+name+"/"+avaticaStatementId+"_"+avaticaConnectionId+"_"+"QueryLogThread_stderr.txt");

//            if(!file.getParentFile().exists()){
//                if(!file.getParentFile().mkdirs())
//                    System.out.println("创建失败");
//            }

            //stdout与stderr保存到本地
            stderr = new PrintStream(file);

            stderr.println("Start getting QueryLog (connectionId=" + avaticaConnectionId + ", statementId=" + avaticaStatementId + ")\n");
            System.err.println("Start getting QueryLog (connectionId="
                    + avaticaConnectionId + ", statementId=" + avaticaStatementId + ")");//stderr
            // 每次拉取operation log的条数
            int batch = 10;
            Service.QueryLogRequest request =
                    new Service.QueryLogRequest(avaticaConnectionId, avaticaStatementId, batch);

            int seqNo = 1;
            int progressNo = 1;
            while (!avaticaStatement.isClosed() && !avaticaStatement.getConnection().isClosed()) {
                Service.QueryLogResponse response = avaticaStatement.getQueryLog(request);
                // 每2秒获取一次
                Thread.sleep(2000);
                if (response != null) {
                    // 打印operation log, 包含执行信息和进度信息
                    List<String> queryLogs = response.logs;
                    if (queryLogs != null && queryLogs.size() > 0) {
                        for (String queryLog : queryLogs) {
                            stderr.println("Statement Query Log #" + (seqNo++) + ": " + queryLog);
//                            stdout.flush();
//                            System.out.println("Statement Query Log #" + (seqNo++) + ": " + queryLog);//stdout
                        }
                    }
                    // 也可以只打印进度信息
                    if (response.progress != null) {
                        String progress = response.progress.getLivyProgress();
                        if (progress != null) {
                            stderr.println("Statement Progress #" + (progressNo++) + ": " + progress);
//                            System.err.println("Statement Progress #" + (progressNo++) + ": " + progress);//stderr
                        }
                    }/* else {
                        stderr.write("QueryLogThread so far obtains no query log from server\n");
//                        System.err.println("QueryLogThread so far obtains no query log from server");//stderr
                    }*/
                }
            }
            stderr.println("Finish getting QueryLog (connectionId="
                    + avaticaConnectionId + ", statementId=" + avaticaStatementId + ")");
//            System.err.println("Finish getting QueryLog (connectionId="
//                    + avaticaConnectionId + ", statementId=" + avaticaStatementId + ")");//stderr
            stderr.flush();

        } catch (Exception e) {
            e.printStackTrace(stderr);
        }
    }
}