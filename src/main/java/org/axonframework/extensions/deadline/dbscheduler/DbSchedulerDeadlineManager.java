package org.axonframework.extensions.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.DeadlineException;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.lifecycle.Phase;
import org.axonframework.lifecycle.ShutdownHandler;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.deadline.GenericDeadlineMessage.asDeadlineMessage;

/**
 * Implementation of {@link DeadlineManager} that delegates scheduling and triggering to a
 * db-scheduler {@link Scheduler}.
 */
public class DbSchedulerDeadlineManager extends AbstractDeadlineManager {
	private static final Logger logger = LoggerFactory.getLogger(DbSchedulerDeadlineManager.class);
	private static final String JOB_NAME_PREFIX = "deadline-";

	private final Scheduler scheduler;
	private final Serializer serializer;

	/**
	 * Instantiate a Builder to be able to create a {@link DbSchedulerDeadlineManager}.
	 *
	 * <p>The {@link TransactionManager} is defaulted to an {@link NoTransactionManager}, and the
	 * {@link Serializer} to a {@link XStreamSerializer}. The {@link Scheduler} and {@link
	 * ScopeAwareProvider} are <b>hard requirements</b> and as such should be provided.
	 *
	 * @return a Builder to be able to create a {@link DbSchedulerDeadlineManager}
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Instantiate a {@link DbSchedulerDeadlineManager} based on the fields contained in the {@link
	 * DbSchedulerDeadlineManager.Builder}.
	 *
	 * <p>Will assert that the {@link Scheduler}, {@link ScopeAwareProvider}, {@link
	 * TransactionManager} and {@link Serializer} are not {@code null}, and will throw an {@link
	 * AxonConfigurationException} if any of them is {@code null}. The TransactionManager,
	 * ScopeAwareProvider and Serializer will be tied to the Scheduler's context. If this
	 * initialization step fails, this will too result in an AxonConfigurationException.
	 *
	 * @param builder the {@link DbSchedulerDeadlineManager.Builder} used to instantiate a {@link
	 *                DbSchedulerDeadlineManager} instance
	 */
	protected DbSchedulerDeadlineManager(DbSchedulerDeadlineManager.Builder builder) {
		builder.validate();
		this.scheduler = builder.scheduler;
		this.serializer = builder.serializer.get();

		try {
			initialize();
		} catch (Exception e) {
			throw new AxonConfigurationException("Unable to initialize DbSchedulerDeadlineManager", e);
		}
	}

	private void initialize() {
		if (!scheduler.getSchedulerState().isStarted()) {
			scheduler.start();
		}
	}

	@Override
	public String schedule(
		Instant triggerDateTime,
		String deadlineName,
		Object messageOrPayload,
		ScopeDescriptor deadlineScope) {
		DeadlineMessage<Object> deadlineMessage =
			asDeadlineMessage(deadlineName, messageOrPayload, triggerDateTime);
		String deadlineId = JOB_NAME_PREFIX + deadlineMessage.getIdentifier();

		runOnPrepareCommitOrNow(
			() -> {
				DeadlineMessage<?> interceptedDeadlineMessage = processDispatchInterceptors(deadlineMessage);
				try {
					scheduler.schedule(
						new TaskInstance<>(
							taskName(deadlineName),
							deadlineId,
							() ->
								DeadlineTask.DeadlineTaskDataBinder.toTaskData(
									serializer, interceptedDeadlineMessage, deadlineScope)),
						triggerDateTime);
				} catch (Exception e) {
					throw new DeadlineException(
						"An error occurred while setting a timer for a deadline", e);
				}
			});

		return deadlineId;
	}

	private String taskName(String deadlineName) {
		return new StringBuilder("axon-deadline-task_").append(deadlineName).toString();
	}

	@Override
	public String schedule(
		Duration triggerDuration,
		String deadlineName,
		Object messageOrPayload,
		ScopeDescriptor deadlineScope) {
		return schedule(
			Instant.now().plus(triggerDuration), deadlineName, messageOrPayload, deadlineScope);
	}

	@Override
	public void cancelSchedule(String deadlineName, String scheduleId) {
		runOnPrepareCommitOrNow(() -> cancelSchedule(
			new TaskInstance<>(
				taskName(deadlineName),
				scheduleId)));
	}

	@Override
	public void cancelAll(String deadlineName) {
		runOnPrepareCommitOrNow(
			() -> {
				try {
					scheduler.fetchScheduledExecutionsForTask(
						taskName(deadlineName),
						TaskDataMap.class,
						scheduledExecution -> {
							cancelSchedule(scheduledExecution.getTaskInstance());
						});
				} catch (Exception e) {
					throw new DeadlineException(
						"An error occurred while cancelling a timer for a deadline manager", e);
				}
			});
	}

	@Override
	public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
		try {
			scheduler.fetchScheduledExecutionsForTask(
				taskName(deadlineName),
				TaskDataMap.class,
				scheduledExecution -> {
					TaskDataMap data = scheduledExecution.getData();
					ScopeDescriptor jobScope =
						DeadlineTask.DeadlineTaskDataBinder.deadlineScope(serializer, data);
					if (scope.equals(jobScope)) {
						cancelSchedule(scheduledExecution.getTaskInstance());
					}
				});
		} catch (Exception e) {
			throw new DeadlineException(
				"An error occurred while cancelling a timer for a deadline manager", e);
		}
	}

	private void cancelSchedule(TaskInstanceId taskInstance) {
		try {
			scheduler.cancel(taskInstance);
		} catch (Exception e) {
			if (e.getMessage().startsWith("Could not cancel schedule")) {
				logger.warn("The task belonging to this token [{}] could not be deleted.", taskInstance);
			}
		}
	}

	@Override
	@ShutdownHandler(phase = Phase.INBOUND_EVENT_CONNECTORS)
	public void shutdown() {
		try {
			scheduler.stop();
		} catch (Exception e) {
			throw new DeadlineException(
				"An error occurred while trying to shutdown the deadline manager", e);
		}
	}

	/**
	 * Builder class to instantiate a {@link DbSchedulerDeadlineManager}.
	 *
	 * <p>The {@link TransactionManager} is defaulted to an {@link NoTransactionManager}, and the
	 * {@link Serializer} to a {@link XStreamSerializer}. The {@link Scheduler} and {@link
	 * ScopeAwareProvider} are <b>hard requirements</b> and as such should be provided.
	 */
	public static class Builder {
		private Scheduler scheduler;
		private Supplier<Serializer> serializer = XStreamSerializer::defaultSerializer;

		/**
		 * Sets the {@link Scheduler} used for scheduling and triggering purposes of the deadlines.
		 *
		 * @param scheduler a {@link Scheduler} used for scheduling and triggering purposes of the
		 *                  deadlines
		 * @return the current Builder instance, for fluent interfacing
		 */
		public DbSchedulerDeadlineManager.Builder scheduler(Scheduler scheduler) {
			assertNonNull(scheduler, "Scheduler may not be null");
			this.scheduler = scheduler;
			return this;
		}

		/**
		 * Sets the {@link Serializer} used to de-/serialize the {@link DeadlineMessage} and the {@link
		 * ScopeDescriptor} into the {@link TaskDataMap}. Defaults to a {@link XStreamSerializer}.
		 *
		 * @param serializer a {@link Serializer} used to de-/serialize the {@link DeadlineMessage} and
		 *                   the {@link ScopeDescriptor} into the {@link TaskDataMap}
		 * @return the current Builder instance, for fluent interfacing
		 */
		public DbSchedulerDeadlineManager.Builder serializer(Serializer serializer) {
			assertNonNull(serializer, "Serializer may not be null");
			this.serializer = () -> serializer;
			return this;
		}

		/**
		 * Initializes a {@link DbSchedulerDeadlineManager} as specified through this Builder.
		 *
		 * @return a {@link DbSchedulerDeadlineManager} as specified through this Builder
		 */
		public DbSchedulerDeadlineManager build() {
			return new DbSchedulerDeadlineManager(this);
		}

		/**
		 * Validates whether the fields contained in this Builder are set accordingly.
		 *
		 * @throws AxonConfigurationException if one field is asserted to be incorrect according to the
		 *                                    Builder's specifications
		 */
		protected void validate() throws AxonConfigurationException {
			assertNonNull(scheduler, "The Scheduler is a hard requirement and should be provided");
		}
	}
}
