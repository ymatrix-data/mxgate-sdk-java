// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: api/gate.proto

package api;

public final class Gate {
  private Gate() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\016api/gate.proto\022\003api\032\025api/gate/mxgate.p" +
      "roto\032\033google/protobuf/empty.proto2U\n\006MXG" +
      "ate\022K\n\017GetMxGateStatus\022\026.google.protobuf" +
      ".Empty\032 .mxgate.GetMxGateStatus.Response" +
      "b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          api.matrix.mxgate.Mxgate.getDescriptor(),
          com.google.protobuf.EmptyProto.getDescriptor(),
        });
    api.matrix.mxgate.Mxgate.getDescriptor();
    com.google.protobuf.EmptyProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
