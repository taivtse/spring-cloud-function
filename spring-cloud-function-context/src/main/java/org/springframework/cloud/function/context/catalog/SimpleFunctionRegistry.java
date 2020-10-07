/*
 * Copyright 2019-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.function.context.catalog;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import net.jodah.typetools.TypeResolver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.ConversionService;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;


/**
 * Implementation of {@link FunctionCatalog} and {@link FunctionRegistry} which
 * does not depend on Spring's {@link BeanFactory}.
 * Each function must be registered with it explicitly to benefit from features
 * such as type conversion, composition, POJO etc.
 *
 * @author Oleg Zhurakousky
 *
 */
public class SimpleFunctionRegistry implements FunctionRegistry, FunctionInspector {
	protected Log logger = LogFactory.getLog(this.getClass());
	/*
	 * - do we care about FunctionRegistration after it's been registered? What additional value does it bring?
	 *
	 */

	private final Field headersField;

	private final Set<FunctionRegistration<?>> functionRegistrations = new HashSet<>();

	private final Map<String, FunctionInvocationWrapper> wrappedFunctionDefinitions = new HashMap<>();

	private final ConversionService conversionService;

	private final CompositeMessageConverter messageConverter;

	private final JsonMapper jsonMapper;

	public SimpleFunctionRegistry(ConversionService conversionService, CompositeMessageConverter messageConverter, JsonMapper jsonMapper) {
		Assert.notNull(messageConverter, "'messageConverter' must not be null");
		Assert.notNull(jsonMapper, "'jsonMapper' must not be null");
		this.conversionService = conversionService;
		this.jsonMapper = jsonMapper;
		this.messageConverter = messageConverter;
		this.headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
		this.headersField.setAccessible(true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T lookup(Class<?> type, String functionDefinition, String... expectedOutputMimeTypes) {
		functionDefinition = this.normalizeFunctionDefinition(functionDefinition);
		FunctionInvocationWrapper function = this.doLookup(type, functionDefinition, expectedOutputMimeTypes);
		if (logger.isInfoEnabled()) {
			if (function != null) {
				logger.info("Located function: " + function);
			}
			else {
				logger.info("Failed to locate function: " + functionDefinition);
			}
		}
		return (T) function;
	}

	@Override
	public FunctionRegistration<?> getRegistration(Object function) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> void register(FunctionRegistration<T> registration) {
		this.functionRegistrations.add(registration);
	}

	//-----

	@Override
	public Set<String> getNames(Class<?> type) {
		return this.functionRegistrations.stream().flatMap(fr -> fr.getNames().stream()).collect(Collectors.toSet());
	}

	@Override
	public int size() {
		return this.functionRegistrations.size();
	}

	/*
	 *
	 */
	protected boolean containsFunction(String functionName) {
		return this.functionRegistrations.stream().anyMatch(reg -> reg.getNames().contains(functionName));
	}

	/*
	 *
	 */
	@SuppressWarnings("unchecked")
	<T> T doLookup(Class<?> type, String functionDefinition, String[] expectedOutputMimeTypes) {
		FunctionInvocationWrapper function = this.wrappedFunctionDefinitions.get(functionDefinition);

		if (function == null) {
			function = this.compose(type, functionDefinition);
		}

		if (function != null) {
			function.expectedOutputContentType = ObjectUtils.isEmpty(expectedOutputMimeTypes) ? null : expectedOutputMimeTypes[0];
		}
		return (T) function;
	}

	/**
	 * This method will make sure that if there is only one function in catalog
	 * it can be looked up by any name of no name.
	 * It does so by attempting to determine the default function name
	 * (the only function in catalog) and checking if it matches the provided name
	 * replacing it if it does not.
	 */
	String normalizeFunctionDefinition(String functionDefinition) {
		functionDefinition = StringUtils.hasText(functionDefinition)
				? functionDefinition.replaceAll(",", "|")
				: System.getProperty(FunctionProperties.FUNCTION_DEFINITION, "");

		if (!this.getNames(null).contains(functionDefinition)) {
			List<String> eligibleFunction = this.getNames(null).stream()
					.filter(name -> !RoutingFunction.FUNCTION_NAME.equals(name))
					.collect(Collectors.toList());
			if (eligibleFunction.size() == 1
					&& !eligibleFunction.get(0).equals(functionDefinition)
					&& !functionDefinition.contains("|")) {
				functionDefinition = eligibleFunction.get(0);
			}
		}
		return functionDefinition;
	}


	private FunctionInvocationWrapper findFunctionInFunctionRegistrations(String functionName) {
		FunctionRegistration<?> functionRegistration = this.functionRegistrations.stream()
				.filter(fr -> fr.getNames().contains(functionName))
				.findFirst()
				.orElseGet(() -> null);
		return functionRegistration != null
				? new FunctionInvocationWrapper(functionName, functionRegistration.getTarget(), functionRegistration.getType().getType())
						: null;

	}

	/*
	 *
	 */
	private FunctionInvocationWrapper compose(Class<?> type, String functionDefinition) {
		String[] functionNames = StringUtils.delimitedListToStringArray(functionDefinition.replaceAll(",", "|").trim(), "|");
		FunctionInvocationWrapper composedFunction = null;

		for (String functionName : functionNames) {
			FunctionInvocationWrapper function = this.findFunctionInFunctionRegistrations(functionName);
			if (function == null) {
				return null;
			}
			else {
				if (composedFunction == null) {
					composedFunction = function;
				}
				else {
					FunctionInvocationWrapper andThenFunction =
							new FunctionInvocationWrapper(functionName, function.getTarget(), function.inputType, function.outputType);
					composedFunction = (FunctionInvocationWrapper) composedFunction.andThen((Function<Object, Object>) andThenFunction);
				}
				this.wrappedFunctionDefinitions.put(composedFunction.functionDefinition, composedFunction);
			}
		}
		return composedFunction;
	}

	/**
	 *
	 */
	@SuppressWarnings("rawtypes")
	public class FunctionInvocationWrapper implements Function<Object, Object>, Consumer<Object>, Supplier<Object>, Runnable {

		private final Object target;

		private Type inputType;

		private final Type outputType;

		private final String functionDefinition;

		private boolean composed;

		private boolean message;

		private String expectedOutputContentType;

		public FunctionInvocationWrapper(String functionDefinition,  Object target, Type functionType) {
			this(functionDefinition, target, FunctionTypeUtils.isSupplier(functionType) ? null : FunctionTypeUtils.getInputType(functionType, 0),
					FunctionTypeUtils.getOutputType(functionType, 0));
		}

		public FunctionInvocationWrapper(String functionDefinition,  Object target, Type inputType, Type outputType) {
			this.target = target;
			this.inputType = inputType == null ? inputType : TypeResolver.reify(inputType);
			this.outputType = outputType == null ? outputType : TypeResolver.reify(outputType);
			this.functionDefinition = functionDefinition;
			this.message = this.inputType != null && FunctionTypeUtils.isMessage(this.inputType);
		}

		public Object getTarget() {
			return target;
		}

		public Type getOutputType() {
			return this.outputType;
		}

		public Type getInputType() {
			return this.inputType;
		}

		public Class<?> getRawOutputType() {
			return TypeResolver.resolveRawClass(this.outputType, null);
		}

		public Class<?> getRawInputType() {
			return TypeResolver.resolveRawClass(this.inputType, null);
		}

		/**
		 *
		 */
		@Override
		public Object apply(Object input) {

			Object result = this.doApply(input);

			if (result != null && this.outputType != null) {
				result = this.convertOutputIfNecessary(result);
			}

			return result;
		}

		@Override
		public Object get() {
			return this.apply(null);
		}

		@Override
		public void accept(Object input) {
			this.apply(input);
		}

		@Override
		public void run() {
			this.apply(null);
		}

		public boolean isConsumer() {
			return this.outputType == null;
		}

		public boolean isSupplier() {
			return this.inputType == null;
		}

		public boolean isFunction() {
			return this.inputType != null && this.outputType != null;
		}

		public boolean isInputTypePublisher() {
			return this.inputType != null && FunctionTypeUtils.isReactive(this.inputType);
		}

		public boolean isOutputTypePublisher() {
			return FunctionTypeUtils.isReactive(this.outputType);
		}

		public boolean isInputTypeMessage() {
			return this.message || this.isRoutingFunction();
		}

		public boolean isOutputTypeMessage() {
			return FunctionTypeUtils.isMessage(this.outputType);
		}


		public boolean isRoutingFunction() {
			return this.target instanceof RoutingFunction;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		@Override
		public <V> Function<Object, V> andThen(Function<? super Object, ? extends V> after) {
			Function f = t -> ((FunctionInvocationWrapper) after).doApply(doApply(t));

			FunctionInvocationWrapper afterWrapper = (FunctionInvocationWrapper) after;

			Type composedFunctionType;
			if (afterWrapper.outputType == null) {
				composedFunctionType = ResolvableType.forClassWithGenerics(Consumer.class, this.inputType == null
						? null
						: ResolvableType.forType(this.inputType)).getType();
			}
			else if (this.inputType == null && afterWrapper.outputType != null) {
				ResolvableType composedOutputType;
				if (FunctionTypeUtils.isFlux(this.outputType)) {
					composedOutputType = ResolvableType.forClassWithGenerics(Flux.class, ResolvableType.forType(afterWrapper.outputType));
				}
				else if (FunctionTypeUtils.isMono(this.outputType)) {
					composedOutputType = ResolvableType.forClassWithGenerics(Mono.class, ResolvableType.forType(afterWrapper.outputType));
				}
				else {
					composedOutputType = ResolvableType.forType(afterWrapper.outputType);
				}

				composedFunctionType = ResolvableType.forClassWithGenerics(Supplier.class, composedOutputType).getType();
			}
			else if (this.outputType == null) {
				throw new IllegalArgumentException("Can NOT compose anything with Consumer");
			}
			else {
				composedFunctionType = ResolvableType.forClassWithGenerics(Function.class,
						ResolvableType.forType(this.inputType),
						ResolvableType.forType(((FunctionInvocationWrapper) after).outputType)).getType();
			}

			String composedName = this.functionDefinition + "|" + afterWrapper.functionDefinition;
			FunctionInvocationWrapper composedFunction = new FunctionInvocationWrapper(composedName, f, composedFunctionType);
			composedFunction.composed = true;

			return (Function<Object, V>) composedFunction;
		}

		/**
		 * Returns true if this function wrapper represents a composed function.
		 * @return true if this function wrapper represents a composed function otherwise false
		 */
		public boolean isComposed() {
			return this.composed;
		}

		/**
		 * Returns the definition of this function.
		 * @return function definition
		 */
		public String getFunctionDefinition() {
			return this.functionDefinition;
		}

		/*
		 *
		 */
		@Override
		public String toString() {
			return this.functionDefinition + (this.isComposed() ? "" : "<" + this.inputType + ", " + this.outputType + ">");
		}

		/**
		 * Will wrap the result in a Message if necessary and will copy input headers to the output message.
		 */
		@SuppressWarnings("unchecked")
		private Object enrichInvocationResultIfNecessary(Object input, Object result) {
			if (input instanceof Message && ((Message) input).getHeaders().containsKey("scf-func-name")) {
				if (result instanceof Message) {
					Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
							.getField(SimpleFunctionRegistry.this.headersField, ((Message) result).getHeaders());
					headersMap.putAll(((Message) input).getHeaders());
				}
				else {
					result = MessageBuilder.withPayload(result).copyHeaders(((Message) input).getHeaders()).build();
				}
			}
			return result;
		}

		/*
		 *
		 */
		private Object fluxifyInputIfNecessary(Object input) {
			if (!(input instanceof Publisher) && this.isInputTypePublisher()) {
				return input == null
						? FunctionTypeUtils.isMono(this.inputType) ? Mono.empty() : Flux.empty()
						: FunctionTypeUtils.isMono(this.inputType) ? Mono.just(input) : Flux.just(input);
			}
			return input;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object doApply(Object input) {
			Object result;

			input = this.fluxifyInputIfNecessary(input);

			Object convertedInput = this.convertInputIfNecessary(input);

			if (this.isRoutingFunction() || this.isComposed()) {
				result = ((Function) this.target).apply(convertedInput);
			}
			else if (this.isSupplier()) {
				result = ((Supplier) this.target).get();
			}
			else if (this.isConsumer()) {
				result = this.invokeConsumer(convertedInput);
			}
			else { // Function
				result = this.invokeFunction(convertedInput);
			}
			return result;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object invokeFunction(Object convertedInput) {
			Object result;
			if (!this.isInputTypePublisher() && convertedInput instanceof Publisher) {
				result = convertedInput instanceof Mono
						? Mono.from((Publisher) convertedInput).map(value -> this.invokeFunctionAndEnrichResultIfNecessary(value))
							.doOnError(ex -> logger.error("Failed to invoke function '" + this.functionDefinition + "'", (Throwable) ex))
						: Flux.from((Publisher) convertedInput).map(value -> this.invokeFunctionAndEnrichResultIfNecessary(value))
							.doOnError(ex -> logger.error("Failed to invoke function '" + this.functionDefinition + "'", (Throwable) ex));
			}
			else {
				result = this.invokeFunctionAndEnrichResultIfNecessary(convertedInput);
			}
			return result;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object invokeFunctionAndEnrichResultIfNecessary(Object value) {
			Object inputValue = value instanceof Tuple2 ? ((Tuple2) value).getT1() : value;

			Object result = ((Function) this.target).apply(inputValue);

			return value instanceof Tuple2
					? this.enrichInvocationResultIfNecessary(((Tuple2) value).getT2(), result)
					: result;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object invokeConsumer(Object convertedInput) {
			Object result = null;
			if (this.isInputTypePublisher()) {
				if (convertedInput instanceof Flux) {
					result = ((Flux) convertedInput)
							.transform(flux -> {
								((Consumer) this.target).accept(flux);
								return Mono.ignoreElements((Flux) flux);
							}).then();
				}
				else {
					result = ((Mono) convertedInput)
							.transform(flux -> {
								((Consumer) this.target).accept(flux);
								return Mono.ignoreElements((Flux) flux);
							}).then();
				}
			}
			else if (convertedInput instanceof Publisher) {
				result = convertedInput instanceof Mono
						? Mono.from((Publisher) convertedInput).doOnNext((Consumer) this.target).then()
						: Flux.from((Publisher) convertedInput).doOnNext((Consumer) this.target).then();
			}
			else {
				((Consumer) this.target).accept(convertedInput);
			}
			return result;
		}

		/*
		 *
		 */
		private Object convertInputIfNecessary(Object input) {
			Object convertedInput = input;
			if (input == null || this.target instanceof RoutingFunction || this.isComposed()) {
				return input;
			}

			if (input instanceof Publisher) {
				convertedInput = this.convertInputPublisherIfNecessary((Publisher) input);
			}
			else if (input instanceof Message) {
				convertedInput = this.convertInputMessageIfNecessary((Message) input);
				convertedInput = this.isPropagateInputHeaders((Message) input) ? Tuples.of(convertedInput, input) : convertedInput;
			}
			else {
				Class<?> inputType = this.isInputTypePublisher() || this.isInputTypeMessage()
						? TypeResolver.resolveRawClass(FunctionTypeUtils.getImmediateGenericType(this.inputType, 0), null)
						: this.getRawInputType();

				convertedInput = this.convertNonMessageInputIfNecessary(inputType, input);
			}
			// wrap in Message if necessary
			if (this.isWrapConvertedInputInMessage(convertedInput)) {
				if (convertedInput instanceof Tuple2) {
					if (!(((Tuple2) convertedInput).getT1() instanceof Message)) {
						convertedInput = Tuples.of(MessageBuilder.withPayload(((Tuple2) convertedInput).getT1()).build(), ((Tuple2) convertedInput).getT2());
					}
				}
				else {
					convertedInput = MessageBuilder.withPayload(convertedInput).build();
				}
			}

			return convertedInput;
		}

		/*
		 *
		 */
		private Object convertNonMessageInputIfNecessary(Class<?> inputType, Object input) {
			Object convertedInput = input;
			if (!inputType.isAssignableFrom(input.getClass())) {
				if (inputType != input.getClass()
						&& SimpleFunctionRegistry.this.conversionService != null
						&& SimpleFunctionRegistry.this.conversionService.canConvert(input.getClass(), inputType)) {
					convertedInput = SimpleFunctionRegistry.this.conversionService.convert(input, inputType);
				}
				else {
					convertedInput = SimpleFunctionRegistry.this.jsonMapper.fromJson(input, inputType);
				}
			}
			return convertedInput;
		}

		/*
		 *
		 */
		private boolean isWrapConvertedInputInMessage(Object convertedInput) {
			return this.inputType != null
					&& FunctionTypeUtils.isMessage(this.inputType)
					&& !(convertedInput instanceof Message)
					&& !(convertedInput instanceof Publisher);
		}

		/*
		 *
		 */
		private boolean isPropagateInputHeaders(Message message) {
			return !this.isInputTypePublisher() && this.isFunction();
		}

		/*
		 *
		 */
		private Object convertInputMessageIfNecessary(Message message) {
			if (message.getPayload() instanceof Optional) {
				return message;
			}
			if (this.inputType == null) {
				return null;
			}
			Object convertedInput = message;
			Type type = FunctionTypeUtils.getGenericType(this.inputType);
			Class rawType = type instanceof ParameterizedType ? (Class) ((ParameterizedType) type).getRawType() : (Class) type;

			convertedInput = FunctionTypeUtils.isTypeCollection(type)
					? SimpleFunctionRegistry.this.messageConverter.fromMessage(message, rawType, type)
					: SimpleFunctionRegistry.this.messageConverter.fromMessage(message, rawType);

			if (this.isInputTypeMessage()) {
				convertedInput = MessageBuilder.withPayload(convertedInput).copyHeaders(message.getHeaders()).build();
			}
			return convertedInput;
		}

		/*
		 *
		 */
		private Object convertOutputIfNecessary(Object output) {
			Object convertedOutput = output;
			if (output instanceof Publisher) {
				convertedOutput = this.convertOutputPublisherIfNecessary((Publisher) output);
			}
			else if (output instanceof Message) {
				convertedOutput = this.convertOutputMessageIfNecessary(output);
			}
			else if (output instanceof Collection && this.isOutputTypeMessage()) {
				convertedOutput = this.convertMultipleOutputValuesIfNecessary(output);
			}
			else if (StringUtils.hasText(this.expectedOutputContentType)) {
				convertedOutput = messageConverter.toMessage(output,
						new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeType.valueOf(this.expectedOutputContentType))));
			}

			return convertedOutput;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object convertOutputMessageIfNecessary(Object output) {
			Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
					.getField(SimpleFunctionRegistry.this.headersField, ((Message) output).getHeaders());
			String expectedContentTypeValue = ((Message) output).getHeaders().containsKey(FunctionProperties.EXPECT_CONTENT_TYPE_HEADER)
					? (String) ((Message) output).getHeaders().get(FunctionProperties.EXPECT_CONTENT_TYPE_HEADER)
							: this.expectedOutputContentType;

			if (StringUtils.hasText(expectedContentTypeValue)) {
				String[] expectedContentTypes = StringUtils.delimitedListToStringArray(expectedContentTypeValue, ",");
				for (String expectedContentType : expectedContentTypes) {
					headersMap.put(MessageHeaders.CONTENT_TYPE, expectedContentType);
					Object result = messageConverter.toMessage(((Message) output).getPayload(), ((Message) output).getHeaders());
					if (result != null) {
						return result;
					}
				}
			}
			return output;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object convertMultipleOutputValuesIfNecessary(Object output) {
			Collection outputCollection = (Collection) output;
			Collection convertedOutputCollection = output instanceof List ? new ArrayList<>() : new TreeSet<>();
			for (Object outToConvert : outputCollection) {
				Object result = this.convertOutputIfNecessary(outToConvert);
				Assert.notNull(result, () -> "Failed to convert output '" + output + "'");
				convertedOutputCollection.add(result);
			}
			return convertedOutputCollection;
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object convertInputPublisherIfNecessary(Publisher publisher) {
			return publisher instanceof Mono
					? Mono.from(publisher).map(this::convertInputIfNecessary).doOnError(ex -> logger.error("Failed to convert input", (Throwable) ex))
					: Flux.from(publisher).map(this::convertInputIfNecessary).doOnError(ex -> logger.error("Failed to convert input", (Throwable) ex));
		}

		/*
		 *
		 */
		@SuppressWarnings("unchecked")
		private Object convertOutputPublisherIfNecessary(Publisher publisher) {
			return publisher instanceof Mono
					? Mono.from(publisher).map(this::convertOutputIfNecessary).doOnError(ex -> logger.error("Failed to convert output", (Throwable) ex))
					: Flux.from(publisher).map(this::convertOutputIfNecessary).doOnError(ex -> logger.error("Failed to convert output", (Throwable) ex));
		}
	}
}
