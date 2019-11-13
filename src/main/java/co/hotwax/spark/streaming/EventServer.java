package co.hotwax.spark.streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EventServer {

	private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
	private static final int PORT = 9999;
	private static final String DELIMITER = ":";
	private static final long EVENT_PERIOD_SECONDS = 5;
	private static final Random random = new Random();

	private static String generateEvent() {
		int userNumber = random.nextInt(10);
		String event = random.nextBoolean() ? "login" : "purchase";
		// In production use a real schema like JSON or protocol buffers

		String generatedEvent = String.format("user-%s", userNumber) + DELIMITER + event;
		System.out.println("putting in this generatedEvent " + generatedEvent);
		return generatedEvent;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		System.out.println("Starting event server ...");
		BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
		SERVER_EXECUTOR.execute(new StreamingServer(eventQueue));
		while (true) {
			eventQueue.put(generateEvent());
			Thread.sleep(TimeUnit.SECONDS.toMillis(EVENT_PERIOD_SECONDS));
		}
	}

	private static class StreamingServer implements Runnable {
		private final BlockingQueue<String> eventQueue;

		public StreamingServer(BlockingQueue<String> eventQueue) {
			this.eventQueue = eventQueue;
		}

		@Override
		public void run() {
			System.out.println("Running Streaming Server");
			try (ServerSocket serverSocket = new ServerSocket(PORT);
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);) {
				System.out.println("Client connected ...");
				while (true) {
					String event = eventQueue.take();
					System.out.println(String.format("Writing \"%s\" to the socket.", event));
					out.println(event);
				}
			} catch (IOException | InterruptedException e) {
				throw new RuntimeException("Server error", e);
			}
		}
	}

}
