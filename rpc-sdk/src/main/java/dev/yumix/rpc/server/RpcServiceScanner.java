package dev.yumix.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.protocol.NsHeadFrame;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Resolves annotated handlers once, before a server starts accepting requests. */
final class RpcServiceScanner {
    private RpcServiceScanner() {}

    static List<Route> scan(Class<?> anchor, Function<Class<?>, Object> instanceFactory) {
        Objects.requireNonNull(anchor, "anchor");
        Objects.requireNonNull(instanceFactory, "instanceFactory");
        String basePackage = anchor.getPackageName();
        if (basePackage.isEmpty()) throw new IllegalArgumentException("Scan anchor must be in a named package");
        ClassLoader loader = anchor.getClassLoader();
        ClassGraph graph = new ClassGraph().enableClassInfo().enableAnnotationInfo()
                .acceptPackages(basePackage);
        if (loader == null) graph.enableClasspath();
        else graph.enableClassLoaders(loader);
        List<Route> routes = new ArrayList<>();
        try (ScanResult result = graph.scan()) {
            List<ClassInfo> classes = new ArrayList<>(
                    result.getClassesWithAnnotation(RpcService.class.getName()));
            classes.sort(Comparator.comparing(ClassInfo::getName));
            for (ClassInfo info : classes) {
                Class<?> type = Class.forName(info.getName(), false, loader);
                routes.addAll(routesFrom(type, instanceFactory));
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("RPC service class could not be loaded", e);
        }
        return routes;
    }

    private static List<Route> routesFrom(Class<?> type, Function<Class<?>, Object> instanceFactory) {
        if (!Modifier.isPublic(type.getModifiers()) || Modifier.isAbstract(type.getModifiers())) {
            throw new IllegalArgumentException("RPC service must be a public concrete class: " + type.getName());
        }
        List<Method> methods = Arrays.stream(type.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(RpcRoute.class))
                .sorted(Comparator.comparing(Method::toGenericString)).toList();
        if (methods.isEmpty()) throw new IllegalArgumentException("RPC service has no routes: " + type.getName());
        Object instance = null;
        if (methods.stream().anyMatch(method -> !Modifier.isStatic(method.getModifiers()))) {
            instance = instanceFactory.apply(type);
            if (!type.isInstance(instance)) {
                throw new IllegalArgumentException("RPC service factory returned an invalid instance: " + type.getName());
            }
        }
        List<Route> routes = new ArrayList<>();
        for (Method method : methods) routes.add(routeFrom(method, instance));
        return routes;
    }

    @SuppressWarnings("unchecked")
    private static Route routeFrom(Method method, Object instance) {
        long methodId = method.getAnnotation(RpcRoute.class).methodId();
        if (methodId <= NsHeadFrame.PING_METHOD_ID || methodId >= NsHeadFrame.PONG_METHOD_ID) {
            throw invalid(method, "method ID is reserved");
        }
        Class<?>[] parameters = method.getParameterTypes();
        if (!Modifier.isPublic(method.getModifiers()) || parameters.length < 1 || parameters.length > 2
                || !Message.class.isAssignableFrom(parameters[0])
                || (parameters.length == 2 && parameters[1] != RpcContext.class)
                || !Message.class.isAssignableFrom(method.getReturnType())) {
            throw invalid(method, "expected public (Message[, RpcContext]) -> Message");
        }
        try {
            Method parserMethod = parameters[0].getMethod("parser");
            if (!Modifier.isStatic(parserMethod.getModifiers())
                    || !Parser.class.isAssignableFrom(parserMethod.getReturnType())) {
                throw invalid(method, "request type has no public static parser()");
            }
            Parser<Message> parser = (Parser<Message>) parserMethod.invoke(null);
            if (parser == null) throw invalid(method, "request parser is null");
            MethodHandle handle = MethodHandles.publicLookup().unreflect(method);
            if (!Modifier.isStatic(method.getModifiers())) handle = handle.bindTo(instance);
            if (parameters.length == 1) handle = MethodHandles.dropArguments(handle, 1, RpcContext.class);
            MethodHandle adapted = handle.asType(MethodType.methodType(
                    Message.class, Message.class, RpcContext.class));
            RpcMethodWithContext<Message, Message> handler = (request, context) -> {
                try {
                    return (Message) adapted.invokeExact((Message) request, context);
                } catch (Exception e) {
                    throw e;
                } catch (Throwable e) {
                    throw new IllegalStateException("RPC handler failed", e);
                }
            };
            return new Route(methodId, parser, handler, method.toGenericString());
        } catch (ReflectiveOperationException e) {
            throw invalid(method, "request parser or handler could not be bound", e);
        }
    }

    static Object instantiate(Class<?> type) {
        try {
            Constructor<?> constructor = type.getConstructor();
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("RPC service needs a public no-argument constructor: "
                    + type.getName(), e);
        }
    }

    private static IllegalArgumentException invalid(Method method, String reason) {
        return new IllegalArgumentException("Invalid RPC route " + method.toGenericString() + ": " + reason);
    }

    private static IllegalArgumentException invalid(Method method, String reason, Throwable cause) {
        return new IllegalArgumentException("Invalid RPC route " + method.toGenericString() + ": " + reason, cause);
    }

    record Route(long methodId, Parser<Message> parser,
                 RpcMethodWithContext<Message, Message> handler, String source) {}
}
