// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: api/supervisor.proto

package api;

public final class Supervisor {
  private Supervisor() {}
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
      "\n\024api/supervisor.proto\022\003api\032\034api/supervi" +
      "sor/program.proto2\333\001\n\007Program\022d\n\017Registe" +
      "rProgram\022\'.api.supervisor.RegisterProgra" +
      "m.Request\032(.api.supervisor.RegisterProgr" +
      "am.Response\022j\n\021UnregisterProgram\022).api.s" +
      "upervisor.UnregisterProgram.Request\032*.ap" +
      "i.supervisor.UnregisterProgram.Responseb" +
      "\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          api.matrix.supervisor.Program.getDescriptor(),
        });
    api.matrix.supervisor.Program.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
