package org.axonframework.extensions.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import org.axonframework.common.AxonNonTransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static org.axonframework.common.ExceptionUtils.findException;

class AxonFailureHandler<T> implements FailureHandler<T> {
	private static final Logger logger = LoggerFactory.getLogger(AxonFailureHandler.class);
	private final Duration sleepDuration;

	public AxonFailureHandler(Duration sleepDuration) {
		this.sleepDuration = sleepDuration;
	}

	@Override
	public void onFailure(
		ExecutionComplete executionComplete, ExecutionOperations<T> executionOperations) {
		executionComplete
			.getCause()
			.filter(
				throwable ->
					findException(throwable, t -> t instanceof AxonNonTransientException).isEmpty())
			.ifPresentOrElse(
				t -> {
					if (logger.isDebugEnabled()) {
						logger.debug(
							"Execution failed. Retrying task {} immediately",
							executionComplete.getExecution().taskInstance);
					}
					executionOperations.reschedule(executionComplete, Instant.now());
				},
				() -> {
					Instant nextTry = Instant.now().plus(sleepDuration);
					if (logger.isDebugEnabled()) {
						logger.debug(
							"Execution failed. Retrying task {} at {}",
							executionComplete.getExecution().taskInstance,
							nextTry);
					}
					executionOperations.reschedule(executionComplete, nextTry);
				});
	}
}
