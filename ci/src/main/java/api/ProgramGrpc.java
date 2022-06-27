package api;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Supervisor APIs, please refer http://supervisord.org/configuration.html#program-x-section-settings
 * for context of supervisor. The original supervisor works based on a local config file, this APIs allows control
 * supervisor with gRPC calls.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.47.0)",
    comments = "Source: api/supervisor.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ProgramGrpc {

  private ProgramGrpc() {}

  public static final String SERVICE_NAME = "api.Program";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<api.matrix.supervisor.Program.RegisterProgram.Request,
      api.matrix.supervisor.Program.RegisterProgram.Response> getRegisterProgramMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterProgram",
      requestType = api.matrix.supervisor.Program.RegisterProgram.Request.class,
      responseType = api.matrix.supervisor.Program.RegisterProgram.Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<api.matrix.supervisor.Program.RegisterProgram.Request,
      api.matrix.supervisor.Program.RegisterProgram.Response> getRegisterProgramMethod() {
    io.grpc.MethodDescriptor<api.matrix.supervisor.Program.RegisterProgram.Request, api.matrix.supervisor.Program.RegisterProgram.Response> getRegisterProgramMethod;
    if ((getRegisterProgramMethod = ProgramGrpc.getRegisterProgramMethod) == null) {
      synchronized (ProgramGrpc.class) {
        if ((getRegisterProgramMethod = ProgramGrpc.getRegisterProgramMethod) == null) {
          ProgramGrpc.getRegisterProgramMethod = getRegisterProgramMethod =
              io.grpc.MethodDescriptor.<api.matrix.supervisor.Program.RegisterProgram.Request, api.matrix.supervisor.Program.RegisterProgram.Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterProgram"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  api.matrix.supervisor.Program.RegisterProgram.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  api.matrix.supervisor.Program.RegisterProgram.Response.getDefaultInstance()))
              .setSchemaDescriptor(new ProgramMethodDescriptorSupplier("RegisterProgram"))
              .build();
        }
      }
    }
    return getRegisterProgramMethod;
  }

  private static volatile io.grpc.MethodDescriptor<api.matrix.supervisor.Program.UnregisterProgram.Request,
      api.matrix.supervisor.Program.UnregisterProgram.Response> getUnregisterProgramMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UnregisterProgram",
      requestType = api.matrix.supervisor.Program.UnregisterProgram.Request.class,
      responseType = api.matrix.supervisor.Program.UnregisterProgram.Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<api.matrix.supervisor.Program.UnregisterProgram.Request,
      api.matrix.supervisor.Program.UnregisterProgram.Response> getUnregisterProgramMethod() {
    io.grpc.MethodDescriptor<api.matrix.supervisor.Program.UnregisterProgram.Request, api.matrix.supervisor.Program.UnregisterProgram.Response> getUnregisterProgramMethod;
    if ((getUnregisterProgramMethod = ProgramGrpc.getUnregisterProgramMethod) == null) {
      synchronized (ProgramGrpc.class) {
        if ((getUnregisterProgramMethod = ProgramGrpc.getUnregisterProgramMethod) == null) {
          ProgramGrpc.getUnregisterProgramMethod = getUnregisterProgramMethod =
              io.grpc.MethodDescriptor.<api.matrix.supervisor.Program.UnregisterProgram.Request, api.matrix.supervisor.Program.UnregisterProgram.Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UnregisterProgram"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  api.matrix.supervisor.Program.UnregisterProgram.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  api.matrix.supervisor.Program.UnregisterProgram.Response.getDefaultInstance()))
              .setSchemaDescriptor(new ProgramMethodDescriptorSupplier("UnregisterProgram"))
              .build();
        }
      }
    }
    return getUnregisterProgramMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ProgramStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProgramStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProgramStub>() {
        @java.lang.Override
        public ProgramStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProgramStub(channel, callOptions);
        }
      };
    return ProgramStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ProgramBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProgramBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProgramBlockingStub>() {
        @java.lang.Override
        public ProgramBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProgramBlockingStub(channel, callOptions);
        }
      };
    return ProgramBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ProgramFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProgramFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProgramFutureStub>() {
        @java.lang.Override
        public ProgramFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProgramFutureStub(channel, callOptions);
        }
      };
    return ProgramFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Supervisor APIs, please refer http://supervisord.org/configuration.html#program-x-section-settings
   * for context of supervisor. The original supervisor works based on a local config file, this APIs allows control
   * supervisor with gRPC calls.
   * </pre>
   */
  public static abstract class ProgramImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * RegisterProgram as a service as a child process of supervisor to run in background.
     * This program is permanently added to supervisor config and can autostart with supervisor.
     * This rpc call finishes as soon as the program changed to running state (if autostart is on).
     * </pre>
     */
    public void registerProgram(api.matrix.supervisor.Program.RegisterProgram.Request request,
        io.grpc.stub.StreamObserver<api.matrix.supervisor.Program.RegisterProgram.Response> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterProgramMethod(), responseObserver);
    }

    /**
     * <pre>
     * UnregisterProgram stop and permanently remove the program from supervisor config.
     * </pre>
     */
    public void unregisterProgram(api.matrix.supervisor.Program.UnregisterProgram.Request request,
        io.grpc.stub.StreamObserver<api.matrix.supervisor.Program.UnregisterProgram.Response> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUnregisterProgramMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRegisterProgramMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                api.matrix.supervisor.Program.RegisterProgram.Request,
                api.matrix.supervisor.Program.RegisterProgram.Response>(
                  this, METHODID_REGISTER_PROGRAM)))
          .addMethod(
            getUnregisterProgramMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                api.matrix.supervisor.Program.UnregisterProgram.Request,
                api.matrix.supervisor.Program.UnregisterProgram.Response>(
                  this, METHODID_UNREGISTER_PROGRAM)))
          .build();
    }
  }

  /**
   * <pre>
   * Supervisor APIs, please refer http://supervisord.org/configuration.html#program-x-section-settings
   * for context of supervisor. The original supervisor works based on a local config file, this APIs allows control
   * supervisor with gRPC calls.
   * </pre>
   */
  public static final class ProgramStub extends io.grpc.stub.AbstractAsyncStub<ProgramStub> {
    private ProgramStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProgramStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProgramStub(channel, callOptions);
    }

    /**
     * <pre>
     * RegisterProgram as a service as a child process of supervisor to run in background.
     * This program is permanently added to supervisor config and can autostart with supervisor.
     * This rpc call finishes as soon as the program changed to running state (if autostart is on).
     * </pre>
     */
    public void registerProgram(api.matrix.supervisor.Program.RegisterProgram.Request request,
        io.grpc.stub.StreamObserver<api.matrix.supervisor.Program.RegisterProgram.Response> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterProgramMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * UnregisterProgram stop and permanently remove the program from supervisor config.
     * </pre>
     */
    public void unregisterProgram(api.matrix.supervisor.Program.UnregisterProgram.Request request,
        io.grpc.stub.StreamObserver<api.matrix.supervisor.Program.UnregisterProgram.Response> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUnregisterProgramMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Supervisor APIs, please refer http://supervisord.org/configuration.html#program-x-section-settings
   * for context of supervisor. The original supervisor works based on a local config file, this APIs allows control
   * supervisor with gRPC calls.
   * </pre>
   */
  public static final class ProgramBlockingStub extends io.grpc.stub.AbstractBlockingStub<ProgramBlockingStub> {
    private ProgramBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProgramBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProgramBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * RegisterProgram as a service as a child process of supervisor to run in background.
     * This program is permanently added to supervisor config and can autostart with supervisor.
     * This rpc call finishes as soon as the program changed to running state (if autostart is on).
     * </pre>
     */
    public api.matrix.supervisor.Program.RegisterProgram.Response registerProgram(api.matrix.supervisor.Program.RegisterProgram.Request request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterProgramMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * UnregisterProgram stop and permanently remove the program from supervisor config.
     * </pre>
     */
    public api.matrix.supervisor.Program.UnregisterProgram.Response unregisterProgram(api.matrix.supervisor.Program.UnregisterProgram.Request request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUnregisterProgramMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Supervisor APIs, please refer http://supervisord.org/configuration.html#program-x-section-settings
   * for context of supervisor. The original supervisor works based on a local config file, this APIs allows control
   * supervisor with gRPC calls.
   * </pre>
   */
  public static final class ProgramFutureStub extends io.grpc.stub.AbstractFutureStub<ProgramFutureStub> {
    private ProgramFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProgramFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProgramFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * RegisterProgram as a service as a child process of supervisor to run in background.
     * This program is permanently added to supervisor config and can autostart with supervisor.
     * This rpc call finishes as soon as the program changed to running state (if autostart is on).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<api.matrix.supervisor.Program.RegisterProgram.Response> registerProgram(
        api.matrix.supervisor.Program.RegisterProgram.Request request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterProgramMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * UnregisterProgram stop and permanently remove the program from supervisor config.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<api.matrix.supervisor.Program.UnregisterProgram.Response> unregisterProgram(
        api.matrix.supervisor.Program.UnregisterProgram.Request request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUnregisterProgramMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_PROGRAM = 0;
  private static final int METHODID_UNREGISTER_PROGRAM = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ProgramImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ProgramImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER_PROGRAM:
          serviceImpl.registerProgram((api.matrix.supervisor.Program.RegisterProgram.Request) request,
              (io.grpc.stub.StreamObserver<api.matrix.supervisor.Program.RegisterProgram.Response>) responseObserver);
          break;
        case METHODID_UNREGISTER_PROGRAM:
          serviceImpl.unregisterProgram((api.matrix.supervisor.Program.UnregisterProgram.Request) request,
              (io.grpc.stub.StreamObserver<api.matrix.supervisor.Program.UnregisterProgram.Response>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ProgramBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ProgramBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return api.Supervisor.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Program");
    }
  }

  private static final class ProgramFileDescriptorSupplier
      extends ProgramBaseDescriptorSupplier {
    ProgramFileDescriptorSupplier() {}
  }

  private static final class ProgramMethodDescriptorSupplier
      extends ProgramBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ProgramMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ProgramGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ProgramFileDescriptorSupplier())
              .addMethod(getRegisterProgramMethod())
              .addMethod(getUnregisterProgramMethod())
              .build();
        }
      }
    }
    return result;
  }
}
