package org.springframework.cloud.function.context.catalog;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.config.FunctionContextUtils;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.env.Environment;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class BeanFactoryAwareFunctionRegistry extends SimpleFunctionRegistry implements ApplicationContextAware {

	private GenericApplicationContext applicationContext;


	public BeanFactoryAwareFunctionRegistry(ConversionService conversionService, CompositeMessageConverter messageConverter, JsonMapper jsonMapper) {
		super(conversionService, messageConverter, jsonMapper);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (GenericApplicationContext) applicationContext;
	}

	/*
	 * Basically gives an approximation only including function registrations and SFC.
	 * Excludes possible POJOs that can be treated as functions
	 */
	@Override
	public int size() {
		return this.applicationContext.getBeanNamesForType(Supplier.class).length +
			this.applicationContext.getBeanNamesForType(Function.class).length +
			this.applicationContext.getBeanNamesForType(Consumer.class).length +
			super.size();
	}

	/*
	 * Doesn't account for POJO so we really don't know until it's been lookedup
	 */
	@Override
	public Set<String> getNames(Class<?> type) {
		Set<String> registeredNames = super.getNames(type);
		if (type == null) {
			registeredNames
				.addAll(Arrays.asList(this.applicationContext.getBeanNamesForType(Function.class)));
			registeredNames
				.addAll(Arrays.asList(this.applicationContext.getBeanNamesForType(Supplier.class)));
			registeredNames
				.addAll(Arrays.asList(this.applicationContext.getBeanNamesForType(Consumer.class)));
		}
		else {
			registeredNames.addAll(Arrays.asList(this.applicationContext.getBeanNamesForType(type)));
		}
		return registeredNames;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T> T lookup(Class<?> type, String functionDefinition, String... expectedOutputMimeTypes) {
		functionDefinition = StringUtils.hasText(functionDefinition)
				? functionDefinition
						: this.applicationContext.getEnvironment().getProperty(FunctionProperties.FUNCTION_DEFINITION, "");

		functionDefinition = this.normalizeFunctionDefinition(functionDefinition);
		if (!StringUtils.hasText(functionDefinition)) {
			logger.debug("Can't determine default function name");
			return null;
		}
		FunctionInvocationWrapper function = this.doLookup(type, functionDefinition, expectedOutputMimeTypes);

		if (function == null) {
			Set<String> functionRegistratioinNames = super.getNames(null);
			String[] functionNames = StringUtils.delimitedListToStringArray(functionDefinition.replaceAll(",", "|").trim(), "|");
			for (String functionName : functionNames) {
				if (functionRegistratioinNames.contains(functionName)) {
					logger.info("Skipping function '" + functionName + "' since it is already present");
				}
				else if (this.applicationContext.containsBean(functionName)) {
					Assert.isTrue(this.applicationContext.containsBean(functionName),
							"Function '" + functionName + "' can't be located in BeanFactory");
					Object functionCandidate = this.applicationContext.getBean(functionName);

					Type functionType = null;
					FunctionRegistration functionRegistration = null;
					if (functionCandidate instanceof FunctionRegistration) {
						functionRegistration = (FunctionRegistration) functionCandidate;
					}
					else if (this.isFunctionPojo(functionCandidate)) {
						Method functionalMethod = FunctionTypeUtils.discoverFunctionalMethod(functionCandidate.getClass());
						functionCandidate = this.proxyTarget(functionCandidate, functionalMethod);
						functionType = FunctionTypeUtils.fromFunctionMethod(functionalMethod);
					}
					else {
						functionType = this.discoverFunctionType(functionCandidate, functionName);
					}
					if (functionRegistration == null) {
						functionRegistration = new FunctionRegistration(functionCandidate, functionName).type(functionType);
					}

					this.register(functionRegistration);
				}
			}
			function = super.doLookup(type, functionDefinition, expectedOutputMimeTypes);
		}

		return (T) function;
	}

	@Override
	protected boolean containsFunction(String functionName) {
		return super.containsFunction(functionName) ? true : this.applicationContext.containsBean(functionName);
	}

	@SuppressWarnings("rawtypes")
	Type discoverFunctionType(Object function, String functionName) {
		if (function instanceof RoutingFunction) {
			return FunctionType.of(FunctionContextUtils.findType(applicationContext.getBeanFactory(), functionName)).getType();
		}
		else if (function instanceof FunctionRegistration) {
			return ((FunctionRegistration) function).getType().getType();
		}
		boolean beanDefinitionExists = false;
		String functionBeanDefinitionName = this.discoverDefinitionName(functionName);
		beanDefinitionExists = this.applicationContext.getBeanFactory().containsBeanDefinition(functionBeanDefinitionName);
		if (this.applicationContext.containsBean("&" + functionName)) {
			Class<?> objectType = this.applicationContext.getBean("&" + functionName, FactoryBean.class)
				.getObjectType();
			return FunctionTypeUtils.discoverFunctionTypeFromClass(objectType);
		}
//		if (!beanDefinitionExists) {
//			logger.info("BeanDefinition for function name(s) '" + Arrays.asList(names) +
//				"' can not be located. FunctionType will be based on " + function.getClass());
//		}

		Type type = FunctionTypeUtils.discoverFunctionTypeFromClass(function.getClass());
		if (beanDefinitionExists) {
			Type t = FunctionTypeUtils.getImmediateGenericType(type, 0);
			if (t == null || t == Object.class) {
				type = FunctionType.of(FunctionContextUtils.findType(this.applicationContext.getBeanFactory(), functionBeanDefinitionName)).getType();
			}
		}
		return type;
	}

	private String discoverDefinitionName(String functionDefinition) {
		String[] aliases = this.applicationContext.getAliases(functionDefinition);
		for (String alias : aliases) {
			if (this.applicationContext.getBeanFactory().containsBeanDefinition(alias)) {
				return alias;
			}
		}
		return functionDefinition;
	}

	private boolean isFunctionPojo(Object function) {
		return !function.getClass().isSynthetic()
			&& !(function instanceof Supplier) && !(function instanceof Function) && !(function instanceof Consumer);
	}

	private Object proxyTarget(Object targetFunction, Method actualMethodToCall) {
		ProxyFactory pf = new ProxyFactory(targetFunction);
		pf.setProxyTargetClass(true);
		pf.setInterfaces(Function.class);
		pf.addAdvice(new MethodInterceptor() {
			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				return actualMethodToCall.invoke(invocation.getThis(), invocation.getArguments());
			}
		});
		return pf.getProxy();
	}
}
