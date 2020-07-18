import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.net.InetSocketAddress;


/**
 *
 */
public class EchoServer {
    private final int port;

    public EchoServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        //配置服务端的NIO线程组！可以指定线程数。
        // Boss线程：由这个线程池提供的线程是boss种类的，用于创建、连接、绑定socket，然后把这些socket传给worker线程池。
        // 在服务器端每个监听的socket都有一个boss线程来处理。在客户端，只有一个boss线程来处理所有的socket。
        EventLoopGroup bossgroup = new NioEventLoopGroup();//传参即可指定线程数

        // Worker线程：Worker线程执行所有的异步I/O，即处理操作；默认线程数是cpucores*2
        EventLoopGroup workergroup = new NioEventLoopGroup();
        try {
            // ServerBootstrap是一个用于设置服务器的引导类,监听端口socket请求
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossgroup,workergroup) //第一个是parent、第二个是child

                    .channel(NioServerSocketChannel.class) // 使用NioServerSocketChannel类，用于实例化新的通道以接受传入连接
                    .localAddress(new InetSocketAddress(port))// 设置服务器监听端口号,后边不用bind了
                    .option(ChannelOption.SO_BACKLOG, 1024)//阻塞队列容量1024
                    /*.childOption(ChannelOption.SO_KEEPALIVE, true)*/.childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS,1000)
                    .option(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(1,1024*1024*1024))
//                    .option(ChannelOption.SO_RCVBUF, 1024*1024*1024)
                    .childHandler(new ChannelInitializer<SocketChannel>() {//
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {

                            ByteBuf delimiter = Unpooled.copiedBuffer("$_".getBytes());

                            ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024*1024, delimiter));//添加解码器
                            ch.pipeline().addLast(new StringEncoder());
                            ch.pipeline().addLast(new StringDecoder());
                            ch.pipeline().addLast(new EchoServerHandler()); // 添加请求处理
                        }
                    });
            // 绑定到端口和启动服务器
            ChannelFuture f = b.bind().sync();
            System.out.println(EchoServer.class.getName() +
                    " started and listening for connections on " + f.channel().localAddress());

//            f.channel()

            f.channel().closeFuture().sync();
        } finally {
            bossgroup.shutdownGracefully().sync();
            workergroup.shutdownGracefully().sync();
        }
    }
}
