package com.yazino.lumberjack;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.net.DefaultSocketConnector;
import ch.qos.logback.core.net.SocketConnector;
import ch.qos.logback.core.util.CloseUtil;
import ch.qos.logback.core.util.Duration;

import javax.net.SocketFactory;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class LogIoAppender<E> extends AppenderBase<E>
        implements Runnable, SocketConnector.ExceptionHandler {

    private static final int DEFAULT_PORT = 28777;
    private static final int DEFAULT_RECONNECTION_DELAY = 30000;
    private static final int DEFAULT_QUEUE_SIZE = 128;
    private static final int DEFAULT_ACCEPT_CONNECTION_DELAY = 5000;
    private static final int DEFAULT_EVENT_DELAY_TIMEOUT = 100;
    private static final String UNKNOWN_STREAM_NAME = "unknown";

    private String remoteHost;
    private int port = DEFAULT_PORT;
    private InetAddress remoteAddress;
    private Duration reconnectionDelay = new Duration(DEFAULT_RECONNECTION_DELAY);
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private int acceptConnectionTimeout = DEFAULT_ACCEPT_CONNECTION_DELAY;
    private Duration eventDelayLimit = new Duration(DEFAULT_EVENT_DELAY_TIMEOUT);
    private String streamName;
    private String nodeName;
    private Encoder<E> encoder;

    private BlockingQueue<E> queue;
    private String peerId;
    private Future<?> task;
    private Future<Socket> connectorTask;

    private volatile Socket socket;

    public void start() {
        if (isStarted()) {
            return;
        }
        int errorCount = 0;
        if (port <= 0) {
            errorCount++;
            addError("No port was configured for appender"
                    + name
                    + " For more information, please visit http://logback.qos.ch/codes.html#socket_no_port");
        }

        if (remoteHost == null) {
            errorCount++;
            addError("No remote host was configured for appender"
                    + name
                    + " For more information, please visit http://logback.qos.ch/codes.html#socket_no_host");
        }

        if (queueSize < 0) {
            errorCount++;
            addError("Queue size must be non-negative");
        }

        if (encoder == null) {
            errorCount++;
            addError("No encoder was configured for appender " + name);
        }

        if (errorCount == 0) {
            try {
                remoteAddress = InetAddress.getByName(remoteHost);
            } catch (UnknownHostException ex) {
                addError("unknown host: " + remoteHost);
                errorCount++;
            }
        }

        if (errorCount == 0) {
            try {
                nodeName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException ex) {
                addError("Failed to lookup localhost");
                errorCount++;
            }
        }

        if (errorCount == 0) {
            queue = newBlockingQueue(queueSize);
            peerId = "remote peer " + remoteHost + ":" + port + ": ";
            task = getContext().getExecutorService().submit(this);
            super.start();
        }
    }

    @Override
    public void stop() {
        if (!isStarted()) {
            return;
        }
        CloseUtil.closeQuietly(socket);
        task.cancel(true);
        if (connectorTask != null) {
            connectorTask.cancel(true);
        }
        super.stop();
    }

    @Override
    protected void append(final E event) {
        if (event == null || !isStarted()) {
            return;
        }

        try {
            final boolean inserted = queue.offer(event, eventDelayLimit.getMilliseconds(), TimeUnit.MILLISECONDS);
            if (!inserted) {
                addInfo("Dropping event due to timeout limit of [" + eventDelayLimit +
                        "] milliseconds being exceeded");
            }
        } catch (InterruptedException e) {
            addError("Interrupted while appending event to SocketAppender", e);
        }
    }

    public final void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                SocketConnector connector = createConnector(remoteAddress, port, 0,
                        reconnectionDelay.getMilliseconds());

                connectorTask = activateConnector(connector);
                if (connectorTask == null) {
                    break;
                }

                socket = waitForConnectorToReturnASocket();
                if (socket == null) {
                    break;
                }
                dispatchEvents();
            }
        } catch (InterruptedException ex) {
            assert true;
        }
        addInfo("shutting down");
    }

    private SocketConnector createConnector(InetAddress address, int port,
                                            int initialDelay, long retryDelay) {
        SocketConnector connector = newConnector(address, port, initialDelay,
                retryDelay);
        connector.setExceptionHandler(this);
        connector.setSocketFactory(getSocketFactory());
        return connector;
    }

    private Future<Socket> activateConnector(SocketConnector connector) {
        try {
            return getContext().getExecutorService().submit(connector);
        } catch (RejectedExecutionException ex) {
            return null;
        }
    }

    private Socket waitForConnectorToReturnASocket() throws InterruptedException {
        try {
            Socket s = connectorTask.get();
            connectorTask = null;
            return s;
        } catch (ExecutionException e) {
            return null;
        }
    }

    private void dispatchEvents() throws InterruptedException {
        try {
            socket.setSoTimeout(acceptConnectionTimeout);
            final BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            socket.setSoTimeout(0);
            addInfo(peerId + "connection established");
            while (true) {
                writeEventTo(out, queue.take());
                out.flush();
            }
        } catch (IOException ex) {
            addInfo(peerId + "connection failed: " + ex);
        } finally {
            CloseUtil.closeQuietly(socket);
            socket = null;
            addInfo(peerId + "connection closed");
        }
    }

    private void writeEventTo(final BufferedWriter out, final E event) throws IOException {
        out.write("+log|");
        out.write(streamName());
        out.write("|");
        out.write(nodeName);
        out.write("|info|");
        out.write(encode(event).replaceAll("\r\n", "\n"));
        out.write("\r\n");
    }

    private String encode(final E event) throws IOException {
        final ByteArrayOutputStream eventOut = new ByteArrayOutputStream();
        try {
            encoder.init(eventOut);
            encoder.doEncode(event);
        } finally {
            try {
                encoder.close();
            } catch (IOException ignored) {
                // ignored
            }
        }
        return eventOut.toString();
    }

    private String streamName() {
        if (streamName == null) {
            return UNKNOWN_STREAM_NAME;
        }
        return streamName;
    }

    public void connectionFailed(SocketConnector connector, Exception ex) {
        if (ex instanceof InterruptedException) {
            addInfo("connector interrupted");
        } else if (ex instanceof ConnectException) {
            addInfo(peerId + "connection refused");
        } else {
            addInfo(peerId + ex);
        }
    }

    /**
     * Creates a new {@link SocketConnector}.
     * <p/>
     * The default implementation creates an instance of {@link ch.qos.logback.core.net.DefaultSocketConnector}.
     * A subclass may override to provide a different {@link SocketConnector}
     * implementation.
     *
     * @param address      target remote remoteAddress
     * @param port         target remote port
     * @param initialDelay delay before the first connection attempt
     * @param retryDelay   delay before a reconnection attempt
     * @return socket connector
     */
    protected SocketConnector newConnector(InetAddress address,
                                           int port, long initialDelay, long retryDelay) {
        return new DefaultSocketConnector(address, port, initialDelay, retryDelay);
    }

    /**
     * Gets the default {@link javax.net.SocketFactory} for the platform.
     * <p/>
     * Subclasses may override to provide a custom socket factory.
     */
    protected SocketFactory getSocketFactory() {
        return SocketFactory.getDefault();
    }

    BlockingQueue<E> newBlockingQueue(int queueSize) {
        if (queueSize <= 0) {
            return new SynchronousQueue<>();
        }
        return new ArrayBlockingQueue<>(queueSize);
    }

    public Encoder<E> getEncoder() {
        return encoder;
    }

    public void setEncoder(final Encoder<E> encoder) {
        this.encoder = encoder;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(final String streamName) {
        this.streamName = streamName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(final String nodeName) {
        this.nodeName = nodeName;
    }

    public void setRemoteHost(String host) {
        remoteHost = host;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setReconnectionDelay(Duration delay) {
        this.reconnectionDelay = delay;
    }

    public Duration getReconnectionDelay() {
        return reconnectionDelay;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setEventDelayLimit(Duration eventDelayLimit) {
        this.eventDelayLimit = eventDelayLimit;
    }

    public Duration getEventDelayLimit() {
        return eventDelayLimit;
    }

    void setAcceptConnectionTimeout(int acceptConnectionTimeout) {
        this.acceptConnectionTimeout = acceptConnectionTimeout;
    }

}

