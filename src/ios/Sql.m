#import "Sql.h"
#import "sqlite3.h"

@interface Sql ()

@property NSValue *db;

@end

@implementation Sql

-(NSString*) getDatabaseDir {
  NSString *libDir =
    [NSSearchPathForDirectoriesInDomains(
       NSLibraryDirectory, NSUserDomainMask, YES)
      objectAtIndex: 0];
  return [libDir stringByAppendingPathComponent:@"NoCloud"];
}

-(id) getPathForDB:(NSString *)dbName
{
  return [[self getDatabaseDir] stringByAppendingPathComponent: dbName];
}


-(NSString *) open
{
  NSString *fullDbPath = [self getPathForDB: @"offline-files.db"];
  const char *sqliteName = [fullDbPath UTF8String];
  sqlite3 *sqldb;
  if (sqlite3_open(sqliteName, &sqldb) == SQLITE_OK) {
    self.db = [NSValue valueWithPointer:sqldb];
    return nil;
  } else {
    return @"cannot open database";
  }
}

- (NSObject*)
 getSqlValueForColumnType:(int)columnType
 withStatement:(sqlite3_stmt*)statement
 withIndex:(int)i
{
  switch (columnType) {
    case SQLITE_INTEGER:
      return [NSNumber numberWithLongLong: sqlite3_column_int64(statement, i)];
    case SQLITE_FLOAT:
      return [NSNumber numberWithDouble: sqlite3_column_double(statement, i)];
    case SQLITE_BLOB:
    case SQLITE_TEXT:
      return [[NSString alloc] initWithBytes:(char *)sqlite3_column_text(statement, i)
                                            length:sqlite3_column_bytes(statement, i)
                                          encoding:NSUTF8StringEncoding];
  }
  return [NSNull null];
}

- (NSArray*)
 executeSql:(NSString*)sql
 withSqlArgs:(NSArray*)sqlArgs
 withReadOnly:(BOOL)readOnly
{
  NSString *error = nil;
  sqlite3_stmt *statement;
  NSMutableArray *resultRows = [NSMutableArray arrayWithCapacity:0];
  NSMutableArray *entry;
  long insertId = 0;
  int rowsAffected = 0;
  int i;

  sqlite3 *sqldb = [self.db pointerValue];

  // compile the statement, throw an error if necessary
  if (sqlite3_prepare_v2(sqldb, [sql UTF8String], -1, &statement, NULL) != SQLITE_OK) {
    error = [Sql convertSQLiteErrorToString:sqldb];
    return @[error];
  }

  bool queryIsReadOnly = sqlite3_stmt_readonly(statement);
  if (readOnly && !queryIsReadOnly) {
    error = [NSString stringWithFormat:@"could not prepare %@", sql];
    return @[error];
  }

  // bind any arguments
  if (sqlArgs != nil) {
    for (i = 0; i < sqlArgs.count; i++) {
      [self bindStatement:statement withArg:[sqlArgs objectAtIndex:i] atIndex:(i + 1)];
    }
  }

  int previousRowsAffected;
  if (!queryIsReadOnly) {
    // calculate the total changes in order to diff later
    previousRowsAffected = sqlite3_total_changes(sqldb);
  }

  // iterate through sql results
  int columnCount;
  NSMutableArray *columnNames = [NSMutableArray arrayWithCapacity:0];
  NSMutableArray *columnTypes;
  NSString *columnName;
  BOOL fetchedColumns = NO;
  int result;
  NSObject *columnValue;
  BOOL hasMore = YES;
  while (hasMore) {
    result = sqlite3_step (statement);
    switch (result) {
      case SQLITE_ROW:
        if (!fetchedColumns) {
          columnCount = sqlite3_column_count(statement);
          for (i = 0; i < columnCount; i++) {
            columnName = [NSString stringWithFormat:@"%s", sqlite3_column_name(statement, i)];
            [columnNames addObject:columnName];
          }
          fetchedColumns = YES;
        }
        columnTypes = [NSMutableArray arrayWithCapacity:0];
        for (i = 0; i < columnCount; i++) {
          int columnType = sqlite3_column_type(statement, i);
          [columnTypes addObject:[NSNumber numberWithInteger:columnType]];
        }
        entry = [NSMutableArray arrayWithCapacity:columnCount];
        for (i = 0; i < columnCount; i++) {
          int columnType = [[columnTypes objectAtIndex:i] intValue];
          columnValue = [self getSqlValueForColumnType:columnType withStatement:statement withIndex: i];
          [entry addObject:columnValue];
        }
        [resultRows addObject:entry];
        break;
    case SQLITE_DONE:
      hasMore = NO;
      break;
    default:
      error = [Sql convertSQLiteErrorToString:sqldb];
      hasMore = NO;
      break;
    }
  }

  if (!queryIsReadOnly) {
    rowsAffected = (sqlite3_total_changes(sqldb) - previousRowsAffected);
    if (rowsAffected > 0) {
      insertId = sqlite3_last_insert_rowid(sqldb);
    }
  }

  sqlite3_finalize (statement);

  if (error) {
    return @[error];
  }
  return @[
    [NSNull null],
    [NSNumber numberWithLong:insertId],
    [NSNumber numberWithInt:rowsAffected],
    columnNames,
    resultRows];
}

-(void)bindStatement:(sqlite3_stmt *)statement withArg:(NSObject *)arg atIndex:(int)argIndex {

    if ([arg isEqual:[NSNull null]]) {
        sqlite3_bind_null(statement, argIndex);
    } else if ([arg isKindOfClass:[NSNumber class]]) {
        NSNumber *numberArg = (NSNumber *)arg;
        const char *numberType = [numberArg objCType];
        if (strcmp(numberType, @encode(int)) == 0 ||
            strcmp(numberType, @encode(long long int)) == 0) {
            sqlite3_bind_int64(statement, argIndex, [numberArg longLongValue]);
        } else if (strcmp(numberType, @encode(double)) == 0) {
            sqlite3_bind_double(statement, argIndex, [numberArg doubleValue]);
        } else {
            sqlite3_bind_text(statement, argIndex, [[arg description] UTF8String], -1, SQLITE_TRANSIENT);
        }
    } else { // NSString
        NSString *stringArg;

        if ([arg isKindOfClass:[NSString class]]) {
            stringArg = (NSString *)arg;
        } else {
            stringArg = [arg description]; // convert to text
        }

        NSData *data = [stringArg dataUsingEncoding:NSUTF8StringEncoding];
        sqlite3_bind_text(statement, argIndex, data.bytes, (int)data.length, SQLITE_TRANSIENT);
    }
}

+(NSString *)convertSQLiteErrorToString:(struct sqlite3 *)sqldb {

    int code = sqlite3_errcode(sqldb);
    const char *cMessage = sqlite3_errmsg(sqldb);
    NSString *message = [[NSString alloc] initWithUTF8String: cMessage];
    return [NSString stringWithFormat:@"Error code %i: %@", code, message];
}

@end
