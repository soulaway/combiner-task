package com.tech.task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.tech.task.Combiner.CombinerException;

public class AppTest {

	private static final long PROPDUSER_A_PRIORITY = 95;
	private static final long PROPDUSER_B_PRIORITY = 5;

	private static final int PROPDUSER_CAPACITY = 100;
	private static final long EMPTY_TIMEOUT = 100;

	/**
	 * Initializing the @Combiner object with 2 producers queues. qA has 100
	 * values with priority 95, qB has 100 values with priority 5; Consumer
	 * thread takes first 100 values from the consumerQ and asserts that 95
	 * values were taken from qA and 5 from qB
	 * 
	 * @throws InterruptedException
	 * @throws CombinerException
	 */

	@Test
	public void testFunctional() throws InterruptedException, CombinerException {
		final SynchronousQueue<Long> consumerQ = new SynchronousQueue<Long>();
		final CombinerImpl<Long> combiner = new CombinerImpl<Long>(consumerQ);

		BlockingQueue<Long> produserA = getQueueWithData(PROPDUSER_A_PRIORITY, PROPDUSER_CAPACITY);
		BlockingQueue<Long> produserB = getQueueWithData(PROPDUSER_B_PRIORITY, PROPDUSER_CAPACITY);

		combiner.addInputQueue(produserA, PROPDUSER_A_PRIORITY, EMPTY_TIMEOUT, TimeUnit.MILLISECONDS);
		combiner.addInputQueue(produserB, PROPDUSER_B_PRIORITY, EMPTY_TIMEOUT, TimeUnit.MILLISECONDS);

		Thread produser = new Thread(combiner);
		produser.start();

		Thread consumer = new Thread() {
			public void run() {
				try {
					Map<Long, Integer> map = new HashMap<Long, Integer>(2);
					for (int i = 0; i < PROPDUSER_CAPACITY; i++) {
						Long key = consumerQ.take();
						map.put(key, (map.containsKey(key)) ? map.get(key) + 1 : 1);
					}
					Assert.assertTrue(map.get(PROPDUSER_A_PRIORITY) == 95);
					Assert.assertTrue(map.get(PROPDUSER_B_PRIORITY) == 5);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		consumer.start();

		wait4Test(1000);
	}

	/**
	 * Initializing the @Combiner object with 2 producers queues. qA and qB has
	 * same priority 95; But different capacity, qA has 30 items loaded, qB has
	 * 100; Consumer thread takes first 100 values from the consumerQ. While
	 * Consuming first 50 values behavior of combining becomes changed: qB will
	 * be deleted from producers and new qC will be added instead. Method
	 * asserts that qA becomes deleted for idling, qB successfully deleted
	 * manually, qC successfully added
	 * 
	 * @throws InterruptedException
	 * @throws CombinerException
	 */
	@Test
	public void testBehavoiur() throws InterruptedException, CombinerException {
		final SynchronousQueue<String> consumerQ = new SynchronousQueue<String>();
		final CombinerImpl<String> combiner = new CombinerImpl<String>(consumerQ);

		BlockingQueue<String> produserA = getQueueWithData("A", 30);
		combiner.addInputQueue(produserA, PROPDUSER_A_PRIORITY, EMPTY_TIMEOUT, TimeUnit.MILLISECONDS);
		BlockingQueue<String> produserB = getQueueWithData("B", PROPDUSER_CAPACITY);
		combiner.addInputQueue(produserB, PROPDUSER_A_PRIORITY, EMPTY_TIMEOUT, TimeUnit.MILLISECONDS);

		Thread produser = new Thread(combiner);
		produser.start();

		Thread consumer = new Thread() {
			public void run() {
				try {
					BlockingQueue<String> produserC = getQueueWithData("C", PROPDUSER_CAPACITY);
					Map<String, Integer> map = new HashMap<String, Integer>(3);
					for (int i = 0; i < PROPDUSER_CAPACITY; i++) {
						if (i == 50) {
							if (combiner.hasInputQueue(produserB)) {
								combiner.removeInputQueue(produserB);
							}
							combiner.addInputQueue(produserC, PROPDUSER_A_PRIORITY, EMPTY_TIMEOUT,
									TimeUnit.MILLISECONDS);

						}
						String key = consumerQ.take();
						map.put(key, (map.containsKey(key)) ? map.get(key) + 1 : 1);

					}
					// queue were deleted for idling after reading all 30 values
					Assert.assertTrue(map.get("A") == 30);
					Assert.assertTrue(!combiner.hasInputQueue(produserA));
					// queue were successfully added
					Assert.assertTrue(combiner.hasInputQueue(produserC));
					// qB has less values taken than qC
					Assert.assertTrue(map.get("B") < map.get("C"));
				} catch (InterruptedException | CombinerException e) {
					e.printStackTrace();
				}
			}
		};
		consumer.start();

		wait4Test(1000);
	}

	/**
	 * Creates the @BlockingQueue with defined capacity and fills it up with
	 * data
	 * 
	 * @param value
	 *            - the value witch feeds the queue
	 * @param capacity
	 *            of the Queue
	 * @return @BlockingQueue
	 * @throws InterruptedException
	 */

	private static <T> BlockingQueue<T> getQueueWithData(T value, int capacity) throws InterruptedException {
		BlockingQueue<T> produser = new ArrayBlockingQueue<T>(capacity);
		for (int n = capacity; n > 0; n--) {
			produser.put(value);
		}
		return produser;
	}

	private static void wait4Test(long timeout) {
		long time = System.currentTimeMillis() + timeout;
		while (System.currentTimeMillis() < time) {
			Thread.yield();
		}
	}
}
