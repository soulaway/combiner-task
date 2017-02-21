package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tech.task.Combiner.CombinerException;

/**
 * Stateful @BlockingQueue wrapper object. Holds a counter, that defines how
 * much polls from this queue the @Combiner will require for the certain polling
 * population.
 * 
 * @author soul
 *
 * @param <T>
 * 			@Combiner parameter
 */
public class QueuePriorityWrapper<T> {

	public final static Logger LOGGER = Logger.getLogger(QueuePriorityWrapper.class.getName());

	private final BlockingQueue<T> queue;
	private final double priority;
	private final long isEmptyTimeout;
	private final TimeUnit timeUnit;
	private final CombinerImpl<T> combiner;

	private double counter;

	public QueuePriorityWrapper(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit,
			CombinerImpl<T> combiner) {
		this.queue = queue;
		this.priority = priority;
		this.counter = priority;
		this.isEmptyTimeout = isEmptyTimeout;
		this.timeUnit = timeUnit;
		this.combiner = combiner;
	}

	/**
	 * Performs a poll of the wrapped queue, decreasing counter. If the polling
	 * population becomes complete will requests to resets all other counters
	 * back to the value of their priority.
	 * 
	 * @return polled value or @null if the queue was idle enough
	 * @throws InterruptedException
	 * @throws CombinerException
	 */

	public T pollQueue() throws InterruptedException, CombinerException {
		T ret = queue.poll(isEmptyTimeout, timeUnit);
		if (ret == null) {
			LOGGER.log(Level.INFO, "queue with priority {0} is being removed because idle", priority);
			combiner.removeInputQueue(queue);
		}
		if (counter <= 0) {
			combiner.resetCounters();
		} else {
			counter--;
		}
		return ret;
	}

	/**
	 * Callback method from the @CombinerImpl
	 */
	protected void resetCounter() {
		counter = priority;
	}

	public BlockingQueue<T> getQueue() {
		return queue;
	}

	public double getPriority() {
		return priority;
	}

	public long getIsEmptyTimeout() {
		return isEmptyTimeout;
	}

	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

	/**
	 * Initially equal to the priority, down counts each poll of the queue,
	 */
	public double getCounter() {
		return counter;
	}

	public void setCounter(double counter) {
		this.counter = counter;
	}
}
