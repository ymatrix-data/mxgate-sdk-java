package api;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.47.0)",
    comments = "Source: api/gate.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class MXGateGrpc {

  private MXGateGrpc() {}

  public static final String SERVICE_NAME = "api.MXGate";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      api.matrix.mxgate.Mxgate.GetMxGateStatus.Response> getGetMxGateStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMxGateStatus",
      requestType = com.google.protobuf.Empty.class,
      responseType = api.matrix.mxgate.Mxgate.GetMxGateStatus.Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      api.matrix.mxgate.Mxgate.GetMxGateStatus.Response> getGetMxGateStatusMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, api.matrix.mxgate.Mxgate.GetMxGateStatus.Response> getGetMxGateStatusMethod;
    if ((getGetMxGateStatusMethod = MXGateGrpc.getGetMxGateStatusMethod) == null) {
      synchronized (MXGateGrpc.class) {
        if ((getGetMxGateStatusMethod = MXGateGrpc.getGetMxGateStatusMethod) == null) {
          MXGateGrpc.getGetMxGateStatusMethod = getGetMxGateStatusMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, api.matrix.mxgate.Mxgate.GetMxGateStatus.Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMxGateStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  api.matrix.mxgate.Mxgate.GetMxGateStatus.Response.getDefaultInstance()))
              .setSchemaDescriptor(new MXGateMethodDescriptorSupplier("GetMxGateStatus"))
              .build();
        }
      }
    }
    return getGetMxGateStatusMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MXGateStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MXGateStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MXGateStub>() {
        @java.lang.Override
        public MXGateStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MXGateStub(channel, callOptions);
        }
      };
    return MXGateStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MXGateBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MXGateBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MXGateBlockingStub>() {
        @java.lang.Override
        public MXGateBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MXGateBlockingStub(channel, callOptions);
        }
      };
    return MXGateBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MXGateFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MXGateFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MXGateFutureStub>() {
        @java.lang.Override
        public MXGateFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MXGateFutureStub(channel, callOptions);
        }
      };
    return MXGateFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class MXGateImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Get MxGate process status(alive, stopped).
     * </pre>
     */
    public void getMxGateStatus(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<api.matrix.mxgate.Mxgate.GetMxGateStatus.Response> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMxGateStatusMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetMxGateStatusMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.google.protobuf.Empty,
                api.matrix.mxgate.Mxgate.GetMxGateStatus.Response>(
                  this, METHODID_GET_MX_GATE_STATUS)))
          .build();
    }
  }

  /**
   */
  public static final class MXGateStub extends io.grpc.stub.AbstractAsyncStub<MXGateStub> {
    private MXGateStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MXGateStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MXGateStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get MxGate process status(alive, stopped).
     * </pre>
     */
    public void getMxGateStatus(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<api.matrix.mxgate.Mxgate.GetMxGateStatus.Response> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMxGateStatusMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MXGateBlockingStub extends io.grpc.stub.AbstractBlockingStub<MXGateBlockingStub> {
    private MXGateBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MXGateBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MXGateBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get MxGate process status(alive, stopped).
     * </pre>
     */
    public api.matrix.mxgate.Mxgate.GetMxGateStatus.Response getMxGateStatus(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMxGateStatusMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MXGateFutureStub extends io.grpc.stub.AbstractFutureStub<MXGateFutureStub> {
    private MXGateFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MXGateFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MXGateFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get MxGate process status(alive, stopped).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<api.matrix.mxgate.Mxgate.GetMxGateStatus.Response> getMxGateStatus(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMxGateStatusMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_MX_GATE_STATUS = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MXGateImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MXGateImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_MX_GATE_STATUS:
          serviceImpl.getMxGateStatus((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<api.matrix.mxgate.Mxgate.GetMxGateStatus.Response>) responseObserver);
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

  private static abstract class MXGateBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MXGateBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return api.Gate.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MXGate");
    }
  }

  private static final class MXGateFileDescriptorSupplier
      extends MXGateBaseDescriptorSupplier {
    MXGateFileDescriptorSupplier() {}
  }

  private static final class MXGateMethodDescriptorSupplier
      extends MXGateBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MXGateMethodDescriptorSupplier(String methodName) {
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
      synchronized (MXGateGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MXGateFileDescriptorSupplier())
              .addMethod(getGetMxGateStatusMethod())
              .build();
        }
      }
    }
    return result;
  }
}
