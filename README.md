# combiner-task

A com.tech.task.Combiner takes items from multiple input queues and feeds them into a single output queue. 
Each input queue has a priority, which determines the approximate frequency at which its items are added to the output queue. 
E.g. if queue A has priority 9.5, and queue B has priority 0.5, then on average, 
for every 100 items added to the output queue, 95 should come from queue A, and 5 should come from queue B.

Input queues can be dynamically added and removed.
