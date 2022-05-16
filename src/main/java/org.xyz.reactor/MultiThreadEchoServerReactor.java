package org.xyz.reactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadEchoServerReactor {
    private final ServerSocketChannel serverSocketChannel;
    private final AtomicInteger next = new AtomicInteger(0);
    private final Selector bossSelector;
    private final Reactor bossReactor;
    private final Selector[] workSelectors = new Selector[2];
    private final Reactor[] workReactors;

    public MultiThreadEchoServerReactor() throws IOException {
        bossSelector = Selector.open();
        workSelectors[0] = Selector.open();
        workSelectors[1] = Selector.open();

        serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 9999);
        serverSocketChannel.socket().bind(address);
        serverSocketChannel.configureBlocking(false);

        SelectionKey sk = serverSocketChannel.register(bossSelector, SelectionKey.OP_ACCEPT);
        sk.attach(new AcceptHandler());

        bossReactor = new Reactor(bossSelector);
        Reactor workReactor1 = new Reactor(workSelectors[0]);
        Reactor workReactor2 = new Reactor(workSelectors[1]);
        workReactors = new Reactor[] {workReactor1, workReactor2};
    }

    public void start() {
        new Thread(bossReactor, "bossReactor").start();
        new Thread(workReactors[0], "workReactor0").start();
        new Thread(workReactors[1], "workReactor1").start();
    }

    class Reactor implements Runnable {
        private Selector selector;

        public Reactor(Selector selector) {
            this.selector = selector;
        }

        public void run() {
            try {
                while (!Thread.interrupted()) {
                    selector.select(1000);
                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    if (null == selectionKeys || selectionKeys.size() == 0) {
                        continue;
                    }
                    Iterator<SelectionKey> it = selectionKeys.iterator();
                    while (it.hasNext()) {
                        SelectionKey sk = it.next();
                        dispatch(sk);
                    }
                    selectionKeys.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void dispatch(SelectionKey sk) {
            Runnable handler = (Runnable) sk.attachment();
            if (null != handler) {
                handler.run();
            }
        }
    }

    class AcceptHandler implements Runnable {

        public void run() {
            try {
                SocketChannel socketChannel = serverSocketChannel.accept();
                if (socketChannel != null) {
                    int index = next.get();
                    Selector selector = workSelectors[index];
                    System.out.println(socketChannel.getRemoteAddress().toString() + ": " + index + " selector");
                    new MultiThreadEchoHandler(selector, socketChannel);
                    System.out.println(socketChannel.getRemoteAddress().toString() + ": " + index + " selector");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (next.incrementAndGet() == workSelectors.length) {
                next.set(0);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        MultiThreadEchoServerReactor reactor = new MultiThreadEchoServerReactor();
        reactor.start();
    }
}

class MultiThreadEchoHandler implements Runnable {
    private final Selector selector;
    private final SocketChannel channel;

    private final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

    static final int RECEIVE = 0, SENDING = 1;

    int state = RECEIVE;

    private final SelectionKey sk;

    static ExecutorService pool = Executors.newFixedThreadPool(10);

    public MultiThreadEchoHandler(Selector selector, SocketChannel channel) throws IOException {
        this.selector = selector;
        this.channel = channel;

        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);

        sk = channel.register(selector, 0);
        sk.attach(this);

        sk.interestOps(SelectionKey.OP_READ);
        selector.wakeup();
    }

    public void run() {
//        syncRun();
        pool.execute(new AsyncTask());
    }

    public void syncRun() {
        try {
            if (state == SENDING) {
                channel.write(byteBuffer);
                byteBuffer.clear();

                sk.interestOps(SelectionKey.OP_READ);
                state = RECEIVE;
            } else if (state == RECEIVE) {
                int length = 0;
                while ((length = channel.read(byteBuffer)) > 0) {
                    System.out.println(new String(byteBuffer.array(), 0, length));
                }
                byteBuffer.flip();

                sk.interestOps(SelectionKey.OP_WRITE);
                state = SENDING;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void asyncRun() {
        try {
            if (state == SENDING) {
                channel.write(byteBuffer);
                byteBuffer.clear();

                sk.interestOps(SelectionKey.OP_READ);
                state = RECEIVE;
            } else if (state == RECEIVE) {
                int length = 0;
                while ((length = channel.read(byteBuffer)) > 0) {
                    System.out.println(new String(byteBuffer.array(), 0, length));
                }
                byteBuffer.flip();

                sk.interestOps(SelectionKey.OP_WRITE);
                state = SENDING;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class AsyncTask implements Runnable {

        public void run() {
            MultiThreadEchoHandler.this.asyncRun();
        }
    }
}
