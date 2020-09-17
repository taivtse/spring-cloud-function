package org.springframework.cloud.function.context.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.config.JsonMessageConverter;
import org.springframework.cloud.function.context.config.NegotiatingMessageConverterWrapper;
import org.springframework.cloud.function.json.GsonMapper;
import org.springframework.cloud.function.json.JacksonMapper;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import reactor.core.publisher.Flux;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SimpleFunctionRegistryTests2 {

	private CompositeMessageConverter messageConverter;

	private ConversionService conversionService;

	@BeforeEach
	public void before() {
		List<MessageConverter> messageConverters = new ArrayList<>();
		JsonMapper jsonMapper = new GsonMapper(new Gson());
		messageConverters.add(new JsonMessageConverter(jsonMapper));
		messageConverters.add(new ByteArrayMessageConverter());
		messageConverters.add(new StringMessageConverter());
		this.messageConverter = new CompositeMessageConverter(messageConverters);

		this.conversionService = new DefaultConversionService();
	}

	@Test
	public void lookup() {
		SimpleFunctionRegistry functionRegistry = new SimpleFunctionRegistry(this.conversionService, this.messageConverter,
				new JacksonMapper(new ObjectMapper()));
		FunctionInvocationWrapper function = functionRegistry.lookup("uppercase");
		assertThat(function).isNull();

		Function userFunction = uppercase();
		FunctionRegistration functionRegistration = new FunctionRegistration(userFunction, "uppercase")
				.type(FunctionType.from(String.class).to(String.class));
		functionRegistry.register(functionRegistration);

		function = functionRegistry.lookup("uppercase");
		assertThat(function).isNotNull();
	}


	@Test
	public void lookupDefaultName() {
		SimpleFunctionRegistry functionRegistry = new SimpleFunctionRegistry(this.conversionService, this.messageConverter,
				new JacksonMapper(new ObjectMapper()));
		Function userFunction = uppercase();
		FunctionRegistration functionRegistration = new FunctionRegistration(userFunction, "uppercase")
				.type(FunctionType.from(String.class).to(String.class));
		functionRegistry.register(functionRegistration);

		FunctionInvocationWrapper function = functionRegistry.lookup("");
		assertThat(function).isNotNull();
	}

	@Test
	public void lookupWithCompositionFunctionAndConsumer() {
		SimpleFunctionRegistry functionRegistry = new SimpleFunctionRegistry(this.conversionService, this.messageConverter,
				new JacksonMapper(new ObjectMapper()));

		Object userFunction = uppercase();
		FunctionRegistration functionRegistration = new FunctionRegistration(userFunction, "uppercase")
				.type(FunctionType.from(String.class).to(String.class));
		functionRegistry.register(functionRegistration);

		userFunction = consumer();
		functionRegistration = new FunctionRegistration(userFunction, "consumer")
				.type(ResolvableType.forClassWithGenerics(Consumer.class, Integer.class).getType());
		functionRegistry.register(functionRegistration);

		FunctionInvocationWrapper functionWrapper = functionRegistry.lookup("uppercase|consumer");

		functionWrapper.apply("123");
	}

	@Test
	public void lookupWithReactiveConsumer() {
		SimpleFunctionRegistry functionRegistry = new SimpleFunctionRegistry(this.conversionService, this.messageConverter,
				new JacksonMapper(new ObjectMapper()));

		Object userFunction = reactiveConsumer();

		FunctionRegistration functionRegistration = new FunctionRegistration(userFunction, "reactiveConsumer")
				.type(ResolvableType.forClassWithGenerics(Consumer.class, ResolvableType.forClassWithGenerics(Flux.class, Integer.class)).getType());
		functionRegistry.register(functionRegistration);

		FunctionInvocationWrapper functionWrapper = functionRegistry.lookup("reactiveConsumer");

		functionWrapper.apply("123");
	}


	public Function<String, String> uppercase() {
		return v -> v.toUpperCase();
	}


	public Function<Object, Integer> hash() {
		return v -> v.hashCode();
	}

	public Supplier<Integer> supplier() {
		return () -> 4;
	}

	public Consumer<Integer> consumer() {
		return System.out::println;
	}

	public Consumer<Flux<Integer>> reactiveConsumer() {
		return flux -> flux.subscribe(v -> {
			System.out.println(v);
		});
	}

//	@EnableAutoConfiguration
//	@Configuration
//	public static class TestConfiguration {
//
//		@Bean
//		public Function<String, String> uppercase() {
//			return v -> v.toUpperCase();
//		}
//
//		@Bean
//		public Function<Flux<String>, Flux<String>> uppercaseReactive() {
//			return flux -> flux.map(v -> v.toUpperCase());
//		}
//	}
}
