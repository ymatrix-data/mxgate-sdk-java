package cn.ymatrix.api;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.47.0)",
    comments = "Source: mxgate_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class MXGateGrpc {

  private MXGateGrpc() {}

  public static final String SERVICE_NAME = "api.MXGate";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<cn.ymatrix.api.GetJobMetadata.Request,
      cn.ymatrix.api.GetJobMetadata.Response> getGetJobMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJobMetadata",
      requestType = cn.ymatrix.api.GetJobMetadata.Request.class,
      responseType = cn.ymatrix.api.GetJobMetadata.Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<cn.ymatrix.api.GetJobMetadata.Request,
      cn.ymatrix.api.GetJobMetadata.Response> getGetJobMetadataMethod() {
    io.grpc.MethodDescriptor<cn.ymatrix.api.GetJobMetadata.Request, cn.ymatrix.api.GetJobMetadata.Response> getGetJobMetadataMethod;
    if ((getGetJobMetadataMethod = MXGateGrpc.getGetJobMetadataMethod) == null) {
      synchronized (MXGateGrpc.class) {
        if ((getGetJobMetadataMethod = MXGateGrpc.getGetJobMetadataMethod) == null) {
          MXGateGrpc.getGetJobMetadataMethod = getGetJobMetadataMethod =
              io.grpc.MethodDescriptor.<cn.ymatrix.api.GetJobMetadata.Request, cn.ymatrix.api.GetJobMetadata.Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJobMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  cn.ymatrix.api.GetJobMetadata.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  cn.ymatrix.api.GetJobMetadata.Response.getDefaultInstance()))
              .setSchemaDescriptor(new MXGateMethodDescriptorSupplier("GetJobMetadata"))
              .build();
        }
      }
    }
    return getGetJobMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<cn.ymatrix.api.SendData.Request,
      cn.ymatrix.api.SendData.Response> getSendDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendData",
      requestType = cn.ymatrix.api.SendData.Request.class,
      responseType = cn.ymatrix.api.SendData.Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<cn.ymatrix.api.SendData.Request,
      cn.ymatrix.api.SendData.Response> getSendDataMethod() {
    io.grpc.MethodDescriptor<cn.ymatrix.api.SendData.Request, cn.ymatrix.api.SendData.Response> getSendDataMethod;
    if ((getSendDataMethod = MXGateGrpc.getSendDataMethod) == null) {
      synchronized (MXGateGrpc.class) {
        if ((getSendDataMethod = MXGateGrpc.getSendDataMethod) == null) {
          MXGateGrpc.getSendDataMethod = getSendDataMethod =
              io.grpc.MethodDescriptor.<cn.ymatrix.api.SendData.Request, cn.ymatrix.api.SendData.Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendData"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  cn.ymatrix.api.SendData.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  cn.ymatrix.api.SendData.Response.getDefaultInstance()))
              .setSchemaDescriptor(new MXGateMethodDescriptorSupplier("SendData"))
              .build();
        }
      }
    }
    return getSendDataMethod;
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
     * Get the target Job Metadata
     * </pre>
     */
    public void getJobMetadata(cn.ymatrix.api.GetJobMetadata.Request request,
        io.grpc.stub.StreamObserver<cn.ymatrix.api.GetJobMetadata.Response> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetJobMetadataMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send data to MxGate
     * </pre>
     */
    public void sendData(cn.ymatrix.api.SendData.Request request,
        io.grpc.stub.StreamObserver<cn.ymatrix.api.SendData.Response> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendDataMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetJobMetadataMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                cn.ymatrix.api.GetJobMetadata.Request,
                cn.ymatrix.api.GetJobMetadata.Response>(
                  this, METHODID_GET_JOB_METADATA)))
          .addMethod(
            getSendDataMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                cn.ymatrix.api.SendData.Request,
                cn.ymatrix.api.SendData.Response>(
                  this, METHODID_SEND_DATA)))
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
     * Get the target Job Metadata
     * </pre>
     */
    public void getJobMetadata(cn.ymatrix.api.GetJobMetadata.Request request,
        io.grpc.stub.StreamObserver<cn.ymatrix.api.GetJobMetadata.Response> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetJobMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send data to MxGate
     * </pre>
     */
    public void sendData(cn.ymatrix.api.SendData.Request request,
        io.grpc.stub.StreamObserver<cn.ymatrix.api.SendData.Response> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendDataMethod(), getCallOptions()), request, responseObserver);
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
     * Get the target Job Metadata
     * </pre>
     */
    public cn.ymatrix.api.GetJobMetadata.Response getJobMetadata(cn.ymatrix.api.GetJobMetadata.Request request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetJobMetadataMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send data to MxGate
     * </pre>
     */
    public cn.ymatrix.api.SendData.Response sendData(cn.ymatrix.api.SendData.Request request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendDataMethod(), getCallOptions(), request);
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
     * Get the target Job Metadata
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<cn.ymatrix.api.GetJobMetadata.Response> getJobMetadata(
        cn.ymatrix.api.GetJobMetadata.Request request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetJobMetadataMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send data to MxGate
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<cn.ymatrix.api.SendData.Response> sendData(
        cn.ymatrix.api.SendData.Request request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendDataMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_JOB_METADATA = 0;
  private static final int METHODID_SEND_DATA = 1;

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
        case METHODID_GET_JOB_METADATA:
          serviceImpl.getJobMetadata((cn.ymatrix.api.GetJobMetadata.Request) request,
              (io.grpc.stub.StreamObserver<cn.ymatrix.api.GetJobMetadata.Response>) responseObserver);
          break;
        case METHODID_SEND_DATA:
          serviceImpl.sendData((cn.ymatrix.api.SendData.Request) request,
              (io.grpc.stub.StreamObserver<cn.ymatrix.api.SendData.Response>) responseObserver);
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
      return cn.ymatrix.api.MxGateService.getDescriptor();
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
              .addMethod(getGetJobMetadataMethod())
              .addMethod(getSendDataMethod())
              .build();
        }
      }
    }
    return result;
  }
}
