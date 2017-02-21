package com.tech.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Runnable combiner processor, that feeds @SynchronousQueue with values from multiple @BlockingQueue
 * During each poll and put calculates most priority @BlockingQueue by sorting the resource list by the value of the priority counter
 * 
 * @author soul
 *
 * @param <T> @Combiner parameter
 */

public class CombinerImpl<T> extends Combiner<T> implements Runnable{
	
	public final static Logger LOGGER = Logger.getLogger(CombinerImpl.class.getName());
	
	List<QueuePriorityWrapper<T>> producers = new ArrayList<QueuePriorityWrapper<T>>();

	protected CombinerImpl(SynchronousQueue<T> outputQueue) {
		super(outputQueue);
	}

	@Override
	public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit)
			throws CombinerException {
		producers.add(new QueuePriorityWrapper<T>(queue, priority, isEmptyTimeout, timeUnit, this));
	}

	@Override
	public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {
		producers.remove(getWrapper(queue).get());
	}

	@Override
	public boolean hasInputQueue(BlockingQueue<T> queue) {
		return getWrapper(queue).isPresent();
	}
	
	/**
	 * Each @QueuePriorityWrapper after feeding enough for its priority, requests all other participants to set their counters back to their initial state
	 */
	protected void resetCounters(){
		producers.stream().forEach(w -> w.resetCounter());
	}
	
	@Override
	public void run() {
		while (producers.size() > 0){
			producers.sort((p1, p2) -> ((p2.getCounter() > p1.getCounter()) ? 1 : -1));
			try {
				T t = producers.stream().findFirst().get().pollQueue();
				if (t != null){
					outputQueue.put(t);
				} // Otherwise queue were just already removed
			} catch (InterruptedException | com.tech.task.Combiner.CombinerException e) {
				e.printStackTrace();
			}
		}
	}
	
	private Optional<QueuePriorityWrapper<T>> getWrapper(BlockingQueue<T> queue){
		return producers.stream().filter((w) -> w.getQueue().equals(queue)).findFirst();
	}
}
