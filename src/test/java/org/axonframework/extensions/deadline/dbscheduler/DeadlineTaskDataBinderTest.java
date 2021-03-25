package org.axonframework.extensions.deadline.dbscheduler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.serialization.JavaSerializer;
import org.axonframework.serialization.SerializedType;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;

import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.axonframework.extensions.deadline.dbscheduler.DeadlineTask.DeadlineTaskDataBinder.*;
import static org.axonframework.messaging.Headers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DeadlineTaskDataBinderTest {

	private static final String TEST_DEADLINE_NAME = "deadline-name";
	private static final String TEST_DEADLINE_PAYLOAD = "deadline-payload";

	private final DeadlineMessage<String> testDeadlineMessage;
	private final MetaData testMetaData;
	private final ScopeDescriptor testDeadlineScope;

	public DeadlineTaskDataBinderTest() {
		DeadlineMessage<String> testDeadlineMessage =
			GenericDeadlineMessage.asDeadlineMessage(TEST_DEADLINE_NAME, TEST_DEADLINE_PAYLOAD, Instant.now());
		testMetaData = MetaData.with("some-key", "some-value");
		this.testDeadlineMessage = testDeadlineMessage.withMetaData(testMetaData);
		testDeadlineScope = new TestScopeDescriptor("aggregate-type", "aggregate-identifier");
	}

	public static Stream<Arguments> serializerImplementationAndAssertionSpecifics() {
		return Stream.of(
			Arguments.arguments(
				spy(JavaSerializer.builder().build()),
				(Function<Class, String>) Class::getName,
				(Predicate<Object>) Objects::nonNull
			),
			Arguments.arguments(
				spy(XStreamSerializer.builder().build()),
				(Function<Class, String>) clazz -> clazz.getSimpleName().toLowerCase(),
				(Predicate<Object>) Objects::isNull
			),
			Arguments.arguments(
				spy(JacksonSerializer.builder().build()),
				(Function<Class, String>) Class::getName,
				(Predicate<Object>) Objects::isNull
			)
		);
	}

	@MethodSource("serializerImplementationAndAssertionSpecifics")
	@ParameterizedTest
	void testToJobData(
		Serializer serializer,
		Function<Class, String> expectedSerializedClassType,
		Predicate<Object> revisionMatcher
	) {
		TaskDataMap result = toTaskData(serializer, testDeadlineMessage, testDeadlineScope);

		assertEquals(TEST_DEADLINE_NAME, result.get(DEADLINE_NAME));
		assertEquals(testDeadlineMessage.getIdentifier(), result.get(MESSAGE_ID));
		assertEquals(testDeadlineMessage.getTimestamp().toString(), result.get(MESSAGE_TIMESTAMP));
		String expectedPayloadType = expectedSerializedClassType.apply(testDeadlineMessage.getPayloadType());
		assertEquals(expectedPayloadType, result.get(MESSAGE_TYPE));
		Object resultRevision = result.get(MESSAGE_REVISION);
		assertTrue(revisionMatcher.test(resultRevision));

		assertNotNull(result.get(SERIALIZED_MESSAGE_PAYLOAD));
		assertNotNull(result.get(MESSAGE_METADATA));
		assertNotNull(result.get(SERIALIZED_DEADLINE_SCOPE));
		assertEquals(testDeadlineScope.getClass().getName(), result.get(SERIALIZED_DEADLINE_SCOPE_CLASS_NAME));

		verify(serializer).serialize(TEST_DEADLINE_PAYLOAD, byte[].class);
		verify(serializer).serialize(testMetaData, byte[].class);
		verify(serializer).serialize(testDeadlineScope, byte[].class);
	}

	@SuppressWarnings("unchecked")
	@MethodSource("serializerImplementationAndAssertionSpecifics")
	@ParameterizedTest
	void testRetrievingDeadlineMessage(
		Serializer serializer,
		Function<Class, String> expectedSerializedClassType,
		Predicate<Object> revisionMatcher
	) {
		TaskDataMap testJobDataMap = toTaskData(serializer, testDeadlineMessage, testDeadlineScope);

		DeadlineMessage<String> result = deadlineMessage(serializer, testJobDataMap);

		assertEquals(testDeadlineMessage.getDeadlineName(), result.getDeadlineName());
		assertEquals(testDeadlineMessage.getIdentifier(), result.getIdentifier());
		assertEquals(testDeadlineMessage.getTimestamp(), result.getTimestamp());
		assertEquals(testDeadlineMessage.getPayload(), result.getPayload());
		assertEquals(testDeadlineMessage.getPayloadType(), result.getPayloadType());
		assertEquals(testDeadlineMessage.getMetaData(), result.getMetaData());

		verify(serializer, times(2))
			.deserialize(argThat(new DeadlineMessageSerializedObjectMatcher(expectedSerializedClassType, revisionMatcher)));
	}

	private static class DeadlineMessageSerializedObjectMatcher implements ArgumentMatcher<SimpleSerializedObject<?>> {
		private final Function<Class, String> expectedSerializedClassType;
		private final Predicate<Object> revisionMatcher;

		DeadlineMessageSerializedObjectMatcher(Function<Class, String> expectedSerializedClassType, Predicate<Object> revisionMatcher) {
			this.expectedSerializedClassType = expectedSerializedClassType;
			this.revisionMatcher = revisionMatcher;
		}

		@Override
		public boolean matches(SimpleSerializedObject<?> serializedObject) {
			String expectedSerializedPayloadType = expectedSerializedClassType.apply(TEST_DEADLINE_PAYLOAD.getClass());

			SerializedType type = serializedObject.getType();
			String serializedTypeName = type.getName();
			boolean isSerializedMetaData = serializedTypeName.equals(MetaData.class.getName());

			return serializedObject.getData() != null &&
				serializedObject.getContentType().equals(byte[].class) &&
				(serializedTypeName.equals(expectedSerializedPayloadType) || isSerializedMetaData) &&
				(isSerializedMetaData || revisionMatcher.test(type.getRevision()));
		}
	}

	@MethodSource("serializerImplementationAndAssertionSpecifics")
	@ParameterizedTest
	void testRetrievingDeadlineScope(Serializer serializer) {
		TaskDataMap testJobDataMap = toTaskData(serializer, testDeadlineMessage, testDeadlineScope);

		ScopeDescriptor result = deadlineScope(serializer, testJobDataMap);

		assertEquals(testDeadlineScope, result);
		verify(serializer).deserialize(
			(SimpleSerializedObject<?>) argThat(this::assertDeadlineScopeSerializedObject)
		);
	}

	private boolean assertDeadlineScopeSerializedObject(SimpleSerializedObject<?> serializedObject) {
		SerializedType type = serializedObject.getType();
		return serializedObject.getData() != null &&
			serializedObject.getContentType().equals(byte[].class) &&
			type.getName().equals(testDeadlineScope.getClass().getName()) &&
			type.getRevision() == null;
	}

	private static class TestScopeDescriptor implements ScopeDescriptor {

		private static final long serialVersionUID = 3584695571254668002L;

		private final String type;
		private Object identifier;

		@JsonCreator
		public TestScopeDescriptor(@JsonProperty("type") String type, @JsonProperty("identifier") Object identifier) {
			this.type = type;
			this.identifier = identifier;
		}

		public String getType() {
			return type;
		}

		public Object getIdentifier() {
			return identifier;
		}

		@Override
		public String scopeDescription() {
			return String.format("TestScopeDescriptor for type [%s] and identifier [%s]", type, identifier);
		}

		@Override
		public int hashCode() {
			return Objects.hash(type, identifier);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
			final TestScopeDescriptor other = (TestScopeDescriptor) obj;
			return Objects.equals(this.type, other.type)
				&& Objects.equals(this.identifier, other.identifier);
		}

		@Override
		public String toString() {
			return "TestScopeDescriptor{" +
				"type=" + type +
				", identifier='" + identifier + '\'' +
				'}';
		}
	}
}
