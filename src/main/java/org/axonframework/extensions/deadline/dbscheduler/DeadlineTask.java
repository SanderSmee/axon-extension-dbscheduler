package org.axonframework.extensions.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.AbstractTask;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.CompletionHandler.OnCompleteRemove;
import com.github.kagkarlsson.scheduler.task.DeadExecutionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleOnStartup;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.messaging.DefaultInterceptorChain;
import org.axonframework.messaging.ExecutionException;
import org.axonframework.messaging.InterceptorChain;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.ResultMessage;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.axonframework.messaging.Headers.*;

public class DeadlineTask extends AbstractTask<TaskDataMap> implements OnStartup {

	private static final Logger logger = LoggerFactory.getLogger(DeadlineTask.class);

	private final Serializer serializer;
	private final TransactionManager transactionManager;
	private final ScopeAwareProvider scopeAwareComponents;
	private final List<MessageHandlerInterceptor<? super DeadlineMessage<?>>> handlerInterceptors;
	private final ScheduleOnStartup<TaskDataMap> scheduleOnStartup;

	public DeadlineTask(
		Serializer serializer,
		TransactionManager transactionManager,
		ScopeAwareProvider scopeAwareComponents,
		List<MessageHandlerInterceptor<? super DeadlineMessage<?>>> handlerInterceptors) {

		super(
			"axon-deadline-task",
			TaskDataMap.class,
			new AxonFailureHandler<>(Duration.ofMinutes(5)),
			new DeadExecutionHandler.ReviveDeadExecution<>());
		this.serializer = serializer;
		this.transactionManager = transactionManager;
		this.scopeAwareComponents = scopeAwareComponents;
		this.handlerInterceptors = handlerInterceptors;
		this.scheduleOnStartup = null;
	}

	@Override
	public CompletionHandler<TaskDataMap> execute(
		TaskInstance<TaskDataMap> taskInstance, ExecutionContext executionContext) {
		if (logger.isDebugEnabled()) {
			logger.debug("Starting a deadline task");
		}

		TaskDataMap taskData = taskInstance.getData();

		DeadlineMessage<?> deadlineMessage =
			DeadlineTaskDataBinder.deadlineMessage(serializer, taskData);
		ScopeDescriptor deadlineScope = DeadlineTaskDataBinder.deadlineScope(serializer, taskData);

		DefaultUnitOfWork<DeadlineMessage<?>> unitOfWork =
			DefaultUnitOfWork.startAndGet(deadlineMessage);
		unitOfWork.attachTransaction(transactionManager);
		InterceptorChain chain =
			new DefaultInterceptorChain<>(
				unitOfWork,
				handlerInterceptors,
				interceptedDeadlineMessage -> {
					executeScheduledDeadline(
						scopeAwareComponents, interceptedDeadlineMessage, deadlineScope);
					return null;
				});

		ResultMessage<?> resultMessage = unitOfWork.executeWithResult(chain::proceed);
		if (resultMessage.isExceptional()) {
			Throwable exceptionResult = resultMessage.exceptionResult();

			logger.error(
				"Exception occurred during processing a deadline task [{}]",
				taskInstance.getTaskName(),
				exceptionResult);
			throw new RuntimeException(exceptionResult);
		} else if (logger.isInfoEnabled()) {
			logger.info(
				"Task successfully executed. Deadline message [{}] processed.",
				deadlineMessage.getPayloadType().getSimpleName());
		}

		return new OnCompleteRemove<>();
	}

	private void executeScheduledDeadline(
		ScopeAwareProvider scopeAwareComponents,
		DeadlineMessage<?> deadlineMessage,
		ScopeDescriptor deadlineScope) {
		scopeAwareComponents
			.provideScopeAwareStream(deadlineScope)
			.filter(scopeAwareComponent -> scopeAwareComponent.canResolve(deadlineScope))
			.forEach(
				scopeAwareComponent -> {
					try {
						scopeAwareComponent.send(deadlineMessage, deadlineScope);
					} catch (Exception e) {
						String exceptionMessage =
							String.format(
								Locale.ENGLISH,
								"Failed to send a DeadlineMessage for scope [%s]",
								deadlineScope.scopeDescription());
						throw new ExecutionException(exceptionMessage, e);
					}
				});
	}

	@Override
	public void onStartup(SchedulerClient schedulerClient, Clock clock) {
		if (scheduleOnStartup != null) {
			scheduleOnStartup.apply(schedulerClient, clock, this);
		}
	}

	/**
	 * This binder is used to map deadline message and deadline scopes to the job data and vice versa.
	 */
	public static class DeadlineTaskDataBinder {
		/**
		 * Key pointing to the serialized deadline {@link ScopeDescriptor} in the {@link TaskDataMap}
		 */
		public static final String SERIALIZED_DEADLINE_SCOPE = "serializedDeadlineScope";
		/**
		 * Key pointing to the class name of the deadline {@link ScopeDescriptor} in the {@link
		 * TaskDataMap}
		 */
		public static final String SERIALIZED_DEADLINE_SCOPE_CLASS_NAME =
			"serializedDeadlineScopeClassName";

		/**
		 * Serializes the provided {@code deadlineMessage} and {@code deadlineScope} and puts them in a
		 * {@link TaskDataMap}.
		 *
		 * @param serializer      the {@link Serializer} used to serialize the given {@code deadlineMessage}
		 *                        and {@code deadlineScope}
		 * @param deadlineMessage the {@link DeadlineMessage} to be handled
		 * @param deadlineScope   the {@link ScopeDescriptor} of the {@link
		 *                        org.axonframework.messaging.Scope} the {@code deadlineMessage} should go to.
		 * @return a {@link TaskDataMap} containing the {@code deadlineMessage} and {@code
		 * deadlineScope}
		 */
		public static TaskDataMap toTaskData(
			Serializer serializer, DeadlineMessage<?> deadlineMessage, ScopeDescriptor deadlineScope) {
			TaskDataMap taskData = new TaskDataMap();
			putDeadlineMessage(taskData, deadlineMessage, serializer);
			putDeadlineScope(taskData, deadlineScope, serializer);
			return taskData;
		}

		@SuppressWarnings("Duplicates")
		private static void putDeadlineMessage(
			TaskDataMap taskData, DeadlineMessage<?> deadlineMessage, Serializer serializer) {
			taskData.put(DEADLINE_NAME, deadlineMessage.getDeadlineName());
			taskData.put(MESSAGE_ID, deadlineMessage.getIdentifier());
			taskData.put(MESSAGE_TIMESTAMP, deadlineMessage.getTimestamp().toString());

			SerializedObject<byte[]> serializedDeadlinePayload =
				serializer.serialize(deadlineMessage.getPayload(), byte[].class);
			taskData.put(SERIALIZED_MESSAGE_PAYLOAD, serializedDeadlinePayload.getData());
			taskData.put(MESSAGE_TYPE, serializedDeadlinePayload.getType().getName());
			taskData.put(MESSAGE_REVISION, serializedDeadlinePayload.getType().getRevision());

			SerializedObject<byte[]> serializedDeadlineMetaData =
				serializer.serialize(deadlineMessage.getMetaData(), byte[].class);
			taskData.put(MESSAGE_METADATA, serializedDeadlineMetaData.getData());
		}

		private static void putDeadlineScope(
			TaskDataMap taskData, ScopeDescriptor deadlineScope, Serializer serializer) {
			SerializedObject<byte[]> serializedDeadlineScope =
				serializer.serialize(deadlineScope, byte[].class);
			taskData.put(SERIALIZED_DEADLINE_SCOPE, serializedDeadlineScope.getData());
			taskData.put(
				SERIALIZED_DEADLINE_SCOPE_CLASS_NAME, serializedDeadlineScope.getType().getName());
		}

		/**
		 * Extracts a {@link DeadlineMessage} from provided {@code taskData}.
		 *
		 * <p><b>Note</b> that this function is able to retrieve two different formats of
		 * DeadlineMessage. The first being a now deprecated solution which used to serialized the
		 * entire {@link DeadlineMessage} into the {@link TaskDataMap}. This is only kept for backwards
		 * compatibility. The second is the new solution which stores all the required deadline fields
		 * separately, only de-/serializing the payload and metadata of a DeadlineMessage instead of the
		 * entire message.
		 *
		 * @param serializer the {@link Serializer} used to deserialize the contents of the given
		 *                   {@code} taskData} into a {@link DeadlineMessage}
		 * @param taskData   the {@link TaskDataMap} which should contain a {@link DeadlineMessage}
		 * @return the {@link DeadlineMessage} pulled from the {@code taskData}
		 */
		public static DeadlineMessage deadlineMessage(
			Serializer serializer, TaskDataMap taskData) {
			return new GenericDeadlineMessage<>(
				(String) taskData.get(DEADLINE_NAME),
				(String) taskData.get(MESSAGE_ID),
				deserializeDeadlinePayload(serializer, taskData),
				deserializeDeadlineMetaData(serializer, taskData),
				retrieveDeadlineTimestamp(taskData));
		}

		private static Object deserializeDeadlinePayload(
			Serializer serializer, TaskDataMap taskData) {
			SimpleSerializedObject<byte[]> serializedDeadlinePayload =
				new SimpleSerializedObject<>(
					(byte[]) taskData.get(SERIALIZED_MESSAGE_PAYLOAD),
					byte[].class,
					(String) taskData.get(MESSAGE_TYPE),
					(String) taskData.get(MESSAGE_REVISION));
			return serializer.deserialize(serializedDeadlinePayload);
		}

		private static Map<String, ?> deserializeDeadlineMetaData(
			Serializer serializer, TaskDataMap taskData) {
			SimpleSerializedObject<byte[]> serializedDeadlineMetaData =
				new SimpleSerializedObject<>(
					(byte[]) taskData.get(MESSAGE_METADATA),
					byte[].class,
					MetaData.class.getName(),
					null);
			return serializer.deserialize(serializedDeadlineMetaData);
		}

		private static Instant retrieveDeadlineTimestamp(TaskDataMap taskData) {
			Object timestamp = taskData.get(MESSAGE_TIMESTAMP);
			if (timestamp instanceof String) {
				return Instant.parse(timestamp.toString());
			}
			return Instant.ofEpochMilli((long) timestamp);
		}

		/**
		 * Extracts a {@link ScopeDescriptor} describing the deadline {@link
		 * org.axonframework.messaging.Scope}, pulled from provided {@code taskData}.
		 *
		 * @param serializer the {@link Serializer} used to deserialize the contents of the given
		 *                   {@code} taskData} into a {@link ScopeDescriptor}
		 * @param taskData   the {@link TaskDataMap} which should contain a {@link ScopeDescriptor}
		 * @return the {@link ScopeDescriptor} describing the deadline {@link
		 * org.axonframework.messaging.Scope}, pulled from provided {@code taskData}
		 */
		public static ScopeDescriptor deadlineScope(Serializer serializer, TaskDataMap taskData) {
			SimpleSerializedObject<byte[]> serializedDeadlineScope =
				new SimpleSerializedObject<>(
					(byte[]) taskData.get(SERIALIZED_DEADLINE_SCOPE),
					byte[].class,
					(String) taskData.get(SERIALIZED_DEADLINE_SCOPE_CLASS_NAME),
					null);
			return serializer.deserialize(serializedDeadlineScope);
		}
	}
}
