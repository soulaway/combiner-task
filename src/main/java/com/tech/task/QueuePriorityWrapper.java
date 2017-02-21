package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tech.task.Combiner.CombinerException;

public class QueuePriorityWrapper<T> {
	
	public final static Logger LOGGER = Logger.getLogger(QueuePriorityWrapper.class.getName());
	
	private final BlockingQueue<T> queue;
	private final double priority;
	private double counter;
	private final long isEmptyTimeout; 
	
	private final TimeUnit timeUnit;
	private final CombinerImpl<T> combiner;

	public QueuePriorityWrapper(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit, CombinerImpl<T> combiner){
		this.queue = queue;
		this.priority = priority;
		this.counter = priority;
		this.isEmptyTimeout = isEmptyTimeout;
		this.timeUnit = timeUnit;
		this.combiner = combiner;
	}
	
	public T pollQueue() throws InterruptedException, CombinerException{
		T ret = queue.poll(isEmptyTimeout, timeUnit);
		if (ret == null){
			LOGGER.log(Level.INFO, "queue with priority {0} is being removed because idle", priority);
			getCombiner().removeInputQueue(queue);
		}
		if (counter == 0){
			getCombiner().resetCounter();
		} else {
			counter--;
		}
		return ret;
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

	public CombinerImpl<T> getCombiner() {
		return combiner;
	}

	public double getCounter() {
		return counter;
	}

	public void setCounter(double counter) {
		this.counter = counter;
	}
}
