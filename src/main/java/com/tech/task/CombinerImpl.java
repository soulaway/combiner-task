package com.tech.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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
		Optional<QueuePriorityWrapper<T>> ow = producers.stream().filter((w) -> w.getQueue().equals(queue)).findFirst();
		if (ow.isPresent()){
			producers.remove(ow.get());
		} else {
			LOGGER.log(Level.WARNING, "unable to remove queue %s - queue is absent", queue.toString());
		}
	}

	@Override
	public boolean hasInputQueue(BlockingQueue<T> queue) {
		Optional<QueuePriorityWrapper<T>> ow = producers.stream().filter((w) -> w.getQueue().equals(queue)).findFirst();
		return ow.isPresent();
	}
	
	public void resetCounter(){
		producers.stream().forEach(w -> w.setCounter(w.getPriority()));
	}
	
	@Override
	public void run() {
		while (producers.size() > 0){
			producers.sort((p1, p2) -> ((p2.getCounter() > p1.getCounter()) ? 1 : -1));
			try {
				T t = producers.get(0).pollQueue();
				if (t != null){
					outputQueue.put(t);
				} // Otherwise queue were already removed
			} catch (InterruptedException | com.tech.task.Combiner.CombinerException e) {
				e.printStackTrace();
			}
		}
	}
}
