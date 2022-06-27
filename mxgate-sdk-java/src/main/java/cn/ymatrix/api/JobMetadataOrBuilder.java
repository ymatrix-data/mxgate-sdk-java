// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: mxgate_service.proto

package cn.ymatrix.api;

public interface JobMetadataOrBuilder extends
    // @@protoc_insertion_point(interface_extends:api.JobMetadata)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string schema = 1;</code>
   * @return The schema.
   */
  java.lang.String getSchema();
  /**
   * <code>string schema = 1;</code>
   * @return The bytes for schema.
   */
  com.google.protobuf.ByteString
      getSchemaBytes();

  /**
   * <code>string table = 2;</code>
   * @return The table.
   */
  java.lang.String getTable();
  /**
   * <code>string table = 2;</code>
   * @return The bytes for table.
   */
  com.google.protobuf.ByteString
      getTableBytes();

  /**
   * <code>string projection = 3;</code>
   * @return The projection.
   */
  java.lang.String getProjection();
  /**
   * <code>string projection = 3;</code>
   * @return The bytes for projection.
   */
  com.google.protobuf.ByteString
      getProjectionBytes();

  /**
   * <code>string insert_projection = 4;</code>
   * @return The insertProjection.
   */
  java.lang.String getInsertProjection();
  /**
   * <code>string insert_projection = 4;</code>
   * @return The bytes for insertProjection.
   */
  com.google.protobuf.ByteString
      getInsertProjectionBytes();

  /**
   * <code>string select_projection = 5;</code>
   * @return The selectProjection.
   */
  java.lang.String getSelectProjection();
  /**
   * <code>string select_projection = 5;</code>
   * @return The bytes for selectProjection.
   */
  com.google.protobuf.ByteString
      getSelectProjectionBytes();

  /**
   * <code>string unique_key_clause = 6;</code>
   * @return The uniqueKeyClause.
   */
  java.lang.String getUniqueKeyClause();
  /**
   * <code>string unique_key_clause = 6;</code>
   * @return The bytes for uniqueKeyClause.
   */
  com.google.protobuf.ByteString
      getUniqueKeyClauseBytes();

  /**
   * <code>string delimiter = 7;</code>
   * @return The delimiter.
   */
  java.lang.String getDelimiter();
  /**
   * <code>string delimiter = 7;</code>
   * @return The bytes for delimiter.
   */
  com.google.protobuf.ByteString
      getDelimiterBytes();

  /**
   * <code>repeated .api.ColumnMeta columns = 8;</code>
   */
  java.util.List<cn.ymatrix.api.ColumnMeta> 
      getColumnsList();
  /**
   * <code>repeated .api.ColumnMeta columns = 8;</code>
   */
  cn.ymatrix.api.ColumnMeta getColumns(int index);
  /**
   * <code>repeated .api.ColumnMeta columns = 8;</code>
   */
  int getColumnsCount();
  /**
   * <code>repeated .api.ColumnMeta columns = 8;</code>
   */
  java.util.List<? extends cn.ymatrix.api.ColumnMetaOrBuilder> 
      getColumnsOrBuilderList();
  /**
   * <code>repeated .api.ColumnMeta columns = 8;</code>
   */
  cn.ymatrix.api.ColumnMetaOrBuilder getColumnsOrBuilder(
      int index);
}