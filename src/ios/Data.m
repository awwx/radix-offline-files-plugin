#import "Data.h"
#import "Sql.h"
#import "lib/Underscore/Underscore.h"

static BOOL debug = NO;

static NSDictionary* serialize(NSObject *obj) {
  NSArray *container = @[obj];

  NSError *error = nil;

  NSData *data =
    [NSJSONSerialization
      dataWithJSONObject:container
      options:0
      error:&error];

  if (error) {
    return @{
      @"error": [NSString
                  stringWithFormat:@"error serializing to JSON: %@", error]
    };
  }

  NSString *string =
    [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];

  string = [string substringToIndex:(string.length - 1)];
  string = [string substringFromIndex:1];

  return @{ @"value": string };
}

static NSDictionary *deserialize(NSString *json) {
  NSData *data = [json dataUsingEncoding:NSUTF8StringEncoding];

  NSError *error = nil;

  id obj =
    [NSJSONSerialization
      JSONObjectWithData:data
      options:NSJSONReadingAllowFragments
      error:&error];

  if (error) {
    return @{
      @"error": [NSString
                  stringWithFormat:@"error deserializing from JSON: %@", error]
    };
  }

  return @{ @"value": obj };
}

static NSString* orNil(NSString *s) {
  if (s)
    return s;
  else
    return @"nil";
}

static NSString* exceptionToString(NSException *exception) {
  // TODO check callStackSymbols nil

  return [[[[orNil(exception.name) stringByAppendingString:@"\n"]
           stringByAppendingString:orNil(exception.reason)]
           stringByAppendingString:@"\n"]
           stringByAppendingString:[exception.callStackSymbols componentsJoinedByString:@"\n"]];
}

static NSDictionary* okResult() {
  return [NSDictionary dictionary];
}

static NSDictionary* valueResult(NSObject *value) {
  return [NSDictionary dictionaryWithObjectsAndKeys:value, @"value", nil];
}

static NSDictionary* errorResult(NSString *error) {
  return [NSDictionary dictionaryWithObjectsAndKeys:error, @"error", nil];
}

static NSDictionary* exceptionResult(NSException *exception) {
  return [NSDictionary dictionaryWithObjectsAndKeys:
           exceptionToString(exception), @"error",
           nil];
}

static BOOL isError(NSDictionary* result) {
  if ([result objectForKey:@"error"])
    return YES;
  else
    return NO;
}

static NSString* sqlErrorResult(NSArray* result) {
  NSObject *error = [result objectAtIndex:0];
  if (error == [NSNull null])
    return nil;
  else if ([error isKindOfClass:[NSString class]])
    return (NSString *)error;
  else
    return @"sql error is not a NSString";
}

@interface Data ()

@property Sql *sql;

@end

@implementation Data

+(NSDictionary *)okResult
{
  return okResult();
}

+(NSDictionary *)errorResult:(NSString *)error
{
  return errorResult(error);
}

+(NSDictionary *)exceptionResult:(NSException *)exception
{
  return exceptionResult(exception);
}

+(BOOL)isError:(NSDictionary *)result
{
  return isError(result);
}

- (id)init {
  self = [super init];
  if (self) {
    self.sql = [[Sql alloc] init];
  }
  return self;
}

+ (NSDictionary *)serialize:(NSObject *)obj
{
  return serialize(obj);
}

+ (NSDictionary *)deserialize:(NSString *)json
{
  return deserialize(json);
}

+(NSArray *)
resultRows:(NSArray *)result
{
  NSArray *columns = [result objectAtIndex:3];
  NSMutableArray *rows = [NSMutableArray arrayWithCapacity:columns.count];

  for (NSArray *rowFields in [result objectAtIndex:4]) {
    [rows addObject:
      [NSMutableDictionary
        dictionaryWithObjects:rowFields
        forKeys:columns]];
  }
  return rows;
}

-(NSDictionary *)
  sql:(NSString *)statement
  args:(NSArray *)args
  readOnly:(BOOL)readOnly
{
  if (debug) {
    NSLog(@"-----------------------------------------------------------------");
    NSLog(@"%@\nargs: %@", statement, args);
  }

  NSArray *result =
    [self.sql
      executeSql:statement
      withSqlArgs:(args ? args : [NSArray array])
      withReadOnly:readOnly];
  NSString *error = sqlErrorResult(result);
  if (error) {
    if (debug)
      NSLog(@"%@", error);
    return @{
      @"error": [[error stringByAppendingString:@"\nin: "]
                  stringByAppendingString:statement]
    };
  } else {
    NSDictionary *r = @{
      @"rows": [Data resultRows:result],
      @"rowsAffected": [result objectAtIndex:2]
    };
    if (debug)
      NSLog(@"%@", r);
    return r;
  }
}

-(NSDictionary *)transaction:(NSDictionary *(^)(void))block
{
  NSDictionary *beginResult = [self sql:@"begin" args:nil readOnly:NO];
  if (isError(beginResult))
    return beginResult;

  NSDictionary *result = nil;

  @try {
    result = block();
  }
  @catch (NSException *exception) {
    NSDictionary *rollbackResult = [self sql:@"rollback" args:nil readOnly:NO];
    if (isError(rollbackResult)) {
      NSLog(@"OfflineFilesPlugin: SQL error while attempting rollback after exception: %@", rollbackResult);
    }
    return exceptionResult(exception);
  }

  if ([result objectForKey:@"noCommit"]) {
    result = Underscore.rejectKeys(result, ^BOOL (NSString *key) {
      return [@"noCommit" isEqual:key];
    });
  } else {
    if (isError(result)) {
      NSDictionary *rollbackResult = [self sql:@"rollback" args:nil readOnly:NO];
      if (isError(rollbackResult)) {
        NSLog(@"OfflineFilesPlugin: SQL error while attempting rollback after error: %@", rollbackResult);
      }
    } else {
      NSDictionary *commitResult = [self sql:@"commit" args:nil readOnly:NO];
      if (isError(commitResult))
        return commitResult;
    }
  }

  return result;
}

-(NSDictionary *)doesTableExist:(NSString *)tableName
{
  NSDictionary *result =
    [self sql:@" select name from sqlite_master       \
                   where type = 'table' and name = ?  "
          args:@[tableName]
          readOnly:YES];
  if (isError(result))
    return result;

  NSDictionary *rows = [result objectForKey:@"rows"];

  return @{
    @"result": [NSNumber numberWithBool:rows.count == 1]
  };
}

+(NSArray*)schema
{
  return @[
    @" create table config (             \
         key text not null primary key,  \
         value text not null             \
       )                                 \
    ",

    @" create table collection (                  \
         collectionId text not null primary key,  \
         path text not null,                      \
         autoUpload integer not null default 0    \
       )                                          ",

    @" create table file (                         \
         fileId text not null primary key,         \
         original integer not null,                \
         collectionId text not null,               \
         filename text not null,                   \
         serverDoc text not null,                  \
         onClient integer not null,                \
         onServer integer not null,                \
         toDownload integer not null,              \
         partition text not null,                  \
         deleted integer not null default 0,       \
         marked integer not null default 0,        \
         uploaded integer not null default 0       \
       )                                           ",


    @" create table job (                   \
         jobId text not null primary key,   \
         collectionId text not null,        \
         notification text not null         \
       )                                    ",

    @" create table jobFile (        \
         jobId text not null,        \
         fileId text not null,       \
         primary key(jobId, fileId)  \
       )                             "

    ];
}

-(NSDictionary *)createSchema
{
  for (NSString *statement in [Data schema]) {
    NSDictionary *result = [self sql:statement args:@[] readOnly:NO];
    if (isError(result))
      return result;
  }

  NSDictionary *setVersionResult = [self setConfig:@"version" value:@"1"];
  if (isError(setVersionResult))
    return setVersionResult;

  return okResult();
}

-(NSDictionary *)
setConfig:(NSString *)key
value:(NSString *)value
{
  NSDictionary *result =
    [self
      sql:@" insert or replace into config (key, value) values (?, ?) "
      args:@[key, value]
      readOnly:NO];
  if (isError(result))
    return result;
  return okResult();
}

-(NSDictionary *)readConfig:(NSString *)key
{
  NSDictionary *result =
    [self
      sql:@" select value from config where key = ? "
      args:@[key]
      readOnly:YES];
  if (isError(result))
    return result;

  NSArray *rows = [result objectForKey:@"rows"];
  if (rows.count == 0) {
    return errorResult([@"key not found in config table: " stringByAppendingString:key]);
  } else if (rows.count == 1) {
    return valueResult([[rows objectAtIndex: 0] objectForKey:@"value"]);
  } else {
    return errorResult([@"multiple rows found for key in config table: " stringByAppendingString:key]);
  }
}

-(NSDictionary *)checkConfigVersion
{
  NSDictionary *versionResult = [self readConfig:@"version"];
  if (isError(versionResult))
    return versionResult;
  NSString *version = [versionResult objectForKey:@"value"];
  if ([@"1" isEqual:version])
    return okResult();
  else
    return errorResult([@"database has an unrecognized version: " stringByAppendingString:version]);
}

-(NSDictionary *)startup
{
  NSString* error = [self.sql open];
  if (error)
    return errorResult(error);

  return
    [self transaction:^{
      NSDictionary *configExists = [self doesTableExist:@"config"];
      if (isError(configExists))
        return configExists;

      if (((NSNumber *)[configExists objectForKey:@"result"]).boolValue) {
        return [self checkConfigVersion];
      } else {
        NSDictionary *createSchemaResult = [self createSchema];
        if (isError(createSchemaResult))
          return createSchemaResult;
        return okResult();
      }
    }];
}

-(NSDictionary *)
configure:(NSDictionary *)configuration
{
  NSDictionary *r1 =
    [self setConfig:@"uploadUrl"
              value:[configuration objectForKey:@"uploadUrl"]];
  if (isError(r1))
    return r1;

  return
    [self setConfig:@"downloadUrl"
              value:[configuration objectForKey:@"downloadUrl"]];
}

-(NSDictionary *)
configureCollection:(NSDictionary *)collection
{
  NSString *collectionId = [collection objectForKey:@"collectionId"];
  NSString *path         = [collection objectForKey:@"path"];

  NSDictionary *r1 =
    [self
      sql:@" select count(*) from collection where collectionId=? "
      args:@[collectionId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  if (((NSNumber *)[[[r1 objectForKey:@"rows"] objectAtIndex:0] objectForKey:@"count(*)"]).boolValue) {
    return
      [self
        sql:@" update collection set path=? where collectionId=? "
        args:@[path, collectionId]
        readOnly:NO];
  } else {
    return
      [self
        sql:@" insert into collection (collectionId, path) \
               values (?, ?)                               "
      args:@[collectionId, path]
      readOnly:NO];
  }
}

-(NSDictionary *)
addOriginal:(NSDictionary *)file
{
  return
    [self
      sql:@"                                                    \
        insert into file                                        \
          (fileId, original, collectionId, filename, serverDoc, \
            onClient, onServer, toDownload, partition)          \
          values (/* fileId       */ ?,                         \
                  /* original     */ 1,                         \
                  /* collectionId */ ?,                         \
                  /* filename     */ ?,                         \
                  /* serverDoc    */ ?,                         \
                  /* onClient     */ 1,                         \
                  /* onServer     */ 0,                         \
                  /* toDownload   */ 0,                         \
                  /* partition    */ ?)                         "
      args:@[
        [file objectForKey:@"fileId"],
        [file objectForKey:@"collectionId"],
        [file objectForKey:@"filename"],
        [file objectForKey:@"serverDoc"],
        [file objectForKey:@"partition"]
      ]
      readOnly:NO];
}

-(NSDictionary *)
 uploadAll:(NSString *)jobId
 collection:(NSString *)collectionId
 notification:(NSString *)notification
{
  return
    [self transaction:^{
      NSDictionary *r1 =
        [self
          sql:@" insert into job                        \
                   (jobId, collectionId, notification)  \
                   values (?, ?, ?)                     "
          args:@[jobId, collectionId, notification]
          readOnly:NO];
      if (isError(r1))
        return r1;

      NSDictionary *r2 =
        [self
          sql:@" insert into jobFile (jobId, fileId)          \
                   select ? as jobId, fileId from file        \
                     where original and collectionId=? and    \
                           onClient and not onServer and      \
                           not uploaded                       "
          args:@[jobId, collectionId]
          readOnly:NO];
      if (isError(r2))
          return r2;

      // If we didn't find any files needing upload, don't
      // actually create an empty task group.

      if ([[r2 objectForKey:@"rowsAffected"] integerValue] == 0) {
        NSDictionary *r3 =
          [self
            sql:@" rollback "
            args:@[jobId]
            readOnly:NO];
        if (isError(r3))
          return r3;
        return @{
          @"noCommit": [NSNumber numberWithBool:YES],
          @"files": @[]
        };
      }

      NSDictionary *r4 =
        [self
          sql:@" select  \
                   fileId,  \
                   file.collectionId,  \
                   filename,  \
                   serverDoc, \
                   collection.path as collectionPath  \
                 from file  \
                 join collection on file.collectionId = collection.collectionId \
                 where fileId in (select fileId from jobFile where jobId=?) "
          args:@[jobId]
          readOnly:YES];
      if (isError(r4))
        return r4;
      return @{
        @"files": [r4 objectForKey:@"rows"]
      };
    }];
}

-(NSDictionary *)
readAllFilesNeedingUpload:(NSString *)collectionId
excluding:(NSArray *)excludeFileIds
{
  NSDictionary *r1 = [self loadExcludeTable:excludeFileIds];
  if (isError(r1))
    return r1;

  NSDictionary *r2 =
    [self
      sql:@" select                                                         \
               fileId,                                                      \
               file.collectionId,                                           \
               filename,                                                    \
               serverDoc,                                                   \
               collection.path as collectionPath                            \
             from file                                                      \
             join collection on file.collectionId = collection.collectionId \
             where file.collectionId=? and original and onClient and        \
                   not onServer and not uploaded and                        \
                   fileId not in (select fileId from excludeFiles)          "
      args:@[collectionId]
      readOnly:YES];
  if (isError(r2)) {
    [self dropExcludeFilesTable];
    return r2;
  }
  NSArray *files = [r2 objectForKey:@"rows"];

  NSDictionary *r3 = [self dropExcludeFilesTable];
  if (isError(r3))
    return r3;

  return @{ @"files":files };
}

-(NSDictionary *)
readAllUploadJobFiles:(NSString *)collectionId
excluding:(NSArray *)excludeFileIds
{
  NSDictionary *r1 = [self loadExcludeTable:excludeFileIds];
  if (isError(r1))
    return r1;

  NSDictionary *r2 =
    [self
      sql:@" select                                                         \
               fileId,                                                      \
               file.collectionId,                                           \
               filename,                                                    \
               serverDoc,                                                   \
               collection.path as collectionPath                            \
             from file                                                      \
             join collection on file.collectionId = collection.collectionId \
             where fileId in (                                              \
                     select fileId from jobFile                             \
                       join job on jobFile.jobId = job.jobId                \
                       where job.collectionId=?) and                        \
                   fileId not in (select fileId from excludeFiles)          "
      args:@[collectionId]
      readOnly:YES];
  if (isError(r2)) {
    [self dropExcludeFilesTable];
    return r2;
  }
  NSArray *files = [r2 objectForKey:@"rows"];

  NSDictionary *r3 = [self dropExcludeFilesTable];
  if (isError(r3))
    return r3;

  return @{ @"files":files };
}


-(NSDictionary *)
readUploads:(NSString *)collectionId
excluding:(NSArray *)excludeFileIds
{
  NSDictionary *r1 = [self readAutoUpload:collectionId];
  if (isError(r1))
    return r1;
  BOOL autoUpload = ((NSNumber *) [r1 objectForKey:@"autoUpload"]).boolValue;

  if (autoUpload) {
    return [self readAllFilesNeedingUpload:collectionId excluding:excludeFileIds];
  } else {
    return [self readAllUploadJobFiles:collectionId excluding:excludeFileIds];
  }
}

-(NSDictionary *)
 uploadPartition:(NSString *)jobId
 collection:(NSString *)collectionId
 partition:(NSString *)partition
 notification:(NSString *)notification
{
  return
    [self transaction:^{
      NSDictionary *r1 =
        [self
          sql:@" insert into job                        \
                   (jobId, collectionId, notification)  \
                   values (?, ?, ?)                     "
          args:@[jobId, collectionId, notification]
          readOnly:NO];
      if (isError(r1))
        return r1;

      NSDictionary *r2 =
        [self
          sql:@" insert into jobFile (jobId, fileId)                  \
                   select ? as jobId, fileId from file                \
                     where original and collectionId=? and            \
                           partition=? and onClient and not onServer  \
                           and not uploaded                           "
          args:@[jobId, collectionId, partition]
          readOnly:NO];
      if (isError(r2))
          return r2;

      // If we didn't find any files needing upload, don't
      // actually create an empty task group.

      if ([[r2 objectForKey:@"rowsAffected"] integerValue] == 0) {
        NSDictionary *r3 =
          [self sql:@" rollback " args:@[jobId] readOnly:NO];
        if (isError(r3))
          return r3;
        return @{
          @"noCommit": [NSNumber numberWithBool:YES],
          @"files": @[]
        };
      }

      NSDictionary *r4 =
        [self
          sql:@" select  \
                   fileId,  \
                   file.collectionId,  \
                   filename,  \
                   serverDoc, \
                   collection.path as collectionPath  \
                 from file  \
                 join collection on file.collectionId = collection.collectionId \
                 where fileId in (select fileId from jobFile where jobId=?) "
          args:@[jobId]
          readOnly:YES];
      if (isError(r4))
        return r4;
      return @{
        @"files": [r4 objectForKey:@"rows"]
      };
    }];
}

-(NSDictionary *)download:(NSString *)fileId
{
  return [self
    sql:@" update file set toDownload=1 where fileId = ? and not deleted "
    args:@[fileId]
    readOnly:NO];
}

-(NSDictionary *)dropExcludeFilesTable
{
  return
    [self
      sql:@" drop table if exists excludeFiles "
      args:@[]
      readOnly:NO];
}

-(NSDictionary *)createExcludeFilesTable
{
  return
    [self
      sql:@" create temporary table excludeFiles( \
               fileId text not null primary key   \
             )                                    "
      args:@[]
      readOnly:NO];
}

-(NSDictionary *)loadExcludeTable:(NSArray *)excludeFileIds
{
  NSDictionary *r0 = [self dropExcludeFilesTable];
  if (isError(r0))
    return r0;

  NSDictionary *r1 = [self createExcludeFilesTable];
  if (isError(r1))
    return r1;

  for (NSString *fileId in excludeFileIds) {
    NSDictionary *r2 =
      [self
        sql:@" insert into excludeFiles (fileId) values (?) "
        args:@[fileId]
        readOnly:NO];
    if (isError(r2)) {
      [self dropExcludeFilesTable];
      return r2;
    }
  }

  return @{};
}

-(NSDictionary *)readDownloadsNotExcluded:(NSString *)collectionId
{
  NSDictionary *r1 =
    [self
      sql:@" select fileId from file                                 \
               where collectionId=? and toDownload and               \
                     fileId not in (select fileId from excludeFiles) "
      args:@[collectionId]
      readOnly:NO];
  if (isError(r1)) {
    [self dropExcludeFilesTable];
    return r1;
  }

  NSArray *fileIds = Underscore.pluck([r1 objectForKey:@"rows"], @"fileId");

  NSDictionary *r2 = [self dropExcludeFilesTable];
  if (isError(r2))
    return r2;

  return @{ @"fileIds": fileIds };
}

-(NSDictionary *)
downloadAll:(NSString *)collectionId
excluding:(NSArray *)excludeFileIds
{
  NSDictionary *r1 = [self loadExcludeTable:excludeFileIds];
  if (isError(r1))
    return r1;

  NSDictionary *r2 =
    [self
      sql:@" update file set toDownload=1                       \
               where collectionId=? and                         \
                     onServer and not onClient and not deleted  "
      args:@[collectionId]
      readOnly:NO];
  if (isError(r2)) {
    [self dropExcludeFilesTable];
    return r2;
  }

  return [self readDownloadsNotExcluded:collectionId];
}

-(NSDictionary *)
readDownloads:(NSString *)collectionId
excluding:(NSArray *)excludeFileIds
{
  NSDictionary *r1 = [self loadExcludeTable:excludeFileIds];
  if (isError(r1))
    return r1;

  return [self readDownloadsNotExcluded:collectionId];
}

-(NSDictionary *)readDownloads
{
  return [self sql:
    @" select                                                            \
           fileId,                                                       \
           file.collectionId,                                            \
           filename,                                                     \
           collection.path as collectionPath,                            \
         from file                                                       \
         join collection on file.collectionId = collection.collectionId  \
         where toDownload and onServer and not onClient and not deleted  "
    args:nil
    readOnly:YES];
}

-(NSDictionary *)
readJobsOfFile:(NSString *)fileId
{
  return
    [self
      sql:@" select distinct jobId from jobFile where fileId=? "
      args:@[fileId]
      readOnly:YES];
}

-(NSDictionary *)
fileTasksComplete:(NSString *)fileId
{
  return
    [self
      sql:@" delete from jobFile where fileId=? "
      args:@[fileId]
      readOnly:NO];
}

-(NSDictionary *)
jobsDone
{
  return
    [self
      sql:@" select jobId, notification from job where       \
               not exists (select fileId from jobFile where  \
                  jobFile.jobId = job.jobId)                 "
      args:@[]
      readOnly:YES];
}

-(NSDictionary *)
removeJob:(NSString *)jobId
{
  return
    [self
      sql:@" delete from job where jobId=? "
      args:@[jobId]
      readOnly:NO];
}

-(NSDictionary *)
uploadComplete:(NSString *)fileId
{
  return
    [self
      sql:@" update file set onServer=1, uploaded=1 where fileId=? "
      args:@[fileId]
      readOnly:NO];
}

+(NSNumber *)asBool:(NSNumber *)n
{
  return [NSNumber numberWithBool:[n integerValue] == 1];
}

-(NSDictionary *)
fileFields:(NSDictionary *)row
{
  return @{
    @"fileId"   : [row objectForKey:@"fileId"],
    @"original" : [Data asBool:[row objectForKey:@"original"]],
    @"filename" : [row objectForKey:@"filename"],
    @"serverDoc": [row objectForKey:@"serverDoc"],
    @"onClient" : [Data asBool:[row objectForKey:@"onClient"]],
    @"onServer" : [Data asBool:[row objectForKey:@"onServer"]],
    @"partition": [row objectForKey:@"partition"],
    @"deleted"  : [Data asBool:[row objectForKey:@"deleted"]]
  };
}

-(NSDictionary *)
readFile:(NSString *)fileId
{
  NSDictionary *r1 =
    [self
      sql:@" select                   \
               fileId,                \
               original,              \
               filename,              \
               serverDoc,             \
               onClient,              \
               onServer,              \
               partition,             \
               deleted                \
             from file where fileId=? "
      args:@[fileId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  NSArray *rows = [r1 objectForKey:@"rows"];
  if (rows.count == 0) {
    return @{ @"file": [NSNull null] };
  } else {
    return @{ @"file":[self fileFields:[rows objectAtIndex:0]] };
  }
}

-(NSDictionary *)
readFilesForPartition:(NSString *)collectionId
partition:(NSString *)partition
{
  NSDictionary *r1 =
    [self
      sql:@" select                                      \
               fileId,                                   \
               original,                                 \
               filename,                                 \
               serverDoc,                                \
               onClient,                                 \
               onServer,                                 \
               partition,                                \
               deleted                                   \
             from file where collectionId=? and          \
               partition=? and not deleted               "
      args:@[collectionId, partition]
      readOnly:YES];
  if (isError(r1))
    return r1;

  return @{
    @"files": Underscore.arrayMap(
      [r1 objectForKey:@"rows"],
      ^(NSDictionary *row) {
        return [self fileFields:row];
      }
    )
  };
}

-(NSDictionary *)
readFileWithCollectionPath:(NSString *)fileId
{
  NSDictionary *r1 =
    [self
      sql:@" select                                                         \
               fileId,                                                      \
               file.collectionId,                                           \
               filename,                                                    \
               collection.path as collectionPath,                           \
               serverDoc,                                                   \
               deleted                                                      \
             from file                                                      \
             join collection on file.collectionId = collection.collectionId \
             where fileId=?                                                 "
      args:@[fileId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  NSArray *rows = [r1 objectForKey:@"rows"];
  if (! rows)
    return @{ @"error": @"readFileWithCollectionPath: result does not contain rows" };
  else if (rows.count == 0) {
    return @{};
  } else {
    return @{ @"file": [rows objectAtIndex:0] };
  }
}

-(NSDictionary *)downloadComplete:(NSString *)fileId
{
  return
    [self
      sql:@" update file set                 \
               toDownload = 0, onClient = 1  \
             where fileId = ?                "
      args:@[fileId]
      readOnly:NO];
}

-(NSDictionary *)
removeAllJobs:(NSString *)collectionId
{
  NSDictionary *r1 =
    [self
      sql:@" delete from jobFile          \
               where jobId in             \
                 (select jobId from job   \
                    where collectionId=?) "
      args:@[collectionId]
      readOnly:NO];
  if (isError(r1))
    return r1;

  NSDictionary *r2 =
    [self
      sql:@" delete from job where collectionId=? "
      args:@[collectionId]
      readOnly:NO];
  if (isError(r2))
    return r2;

  return okResult();
}

-(NSDictionary *)
switchToAutoUploadOn:(NSString *)collectionId
{
  return
    [self transaction:^{
      NSDictionary *r1 =
        [self
          sql:@" update collection set autoUpload=1 where collectionId=? "
          args:@[collectionId]
          readOnly:NO];
      if (isError(r1))
        return r1;

      NSDictionary *r2 = [self removeAllJobs:collectionId];
      if (isError(r2))
        return r2;

      return @{};
    }];
}

-(NSDictionary *)
switchToAutoUploadOff:(NSString *)collectionId
{
  NSDictionary *r1 =
    [self
      sql:@" update collection set autoUpload=0 where collectionId=? "
      args:@[collectionId]
      readOnly:NO];
  if (isError(r1))
    return r1;

  return okResult();
}

+(NSDictionary *)singleRow:(NSDictionary *)r
{
  NSArray *rows = [r objectForKey:@"rows"];
  if (! rows)
    return errorResult(@"result does not contain rows");
  if (rows.count != 1)
    return errorResult(@"result did not contain a single row");
  return @{@"row": [rows objectAtIndex:0]};
}

-(NSDictionary *)
readAutoUpload:(NSString *)collectionId
{
  NSDictionary *r1 =
    [self
      sql:@" select autoUpload from collection where collectionId=? "
      args:@[collectionId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  NSDictionary *r2 = [Data singleRow:r1];
  if (isError(r2))
    return r2;
  NSDictionary *row = [r2 objectForKey:@"row"];

  return @{@"autoUpload": [row objectForKey:@"autoUpload"]};
}

-(NSDictionary *)
convertToDownload:(NSString *)fileId
{
  return [self
    sql:@" update file set original=0, uploaded=0, toDownload=0 where fileId=? "
    args:@[fileId]
    readOnly:NO];
}

-(NSDictionary *)
mergeServerDoc:(NSDictionary *)info
{
  NSString *fileId = [info objectForKey:@"fileId"];

  NSDictionary *r1 =
    [self
      sql:@" select fileId from file where fileId=? "
      args:@[fileId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  NSArray *rows = [r1 objectForKey:@"rows"];
  if (rows.count == 0) {
    // no local file, new server doc
    NSDictionary *r2 = [self
      sql: @" insert into file                                          \
                (fileId, original, collectionId, filename, serverDoc,   \
                 onClient, onServer, toDownload, partition, marked)     \
                values (                                                \
                 /* fileId */       ?,                                  \
                 /* original */     0,                                  \
                 /* collectionId */ ?,                                  \
                 /* filename */     ?,                                  \
                 /* serverDoc */    ?,                                  \
                 /* onClient */     0,                                  \
                 /* onServer */     1,                                  \
                 /* toDownload */   0,                                  \
                 /* partition */    ?,                                  \
                 /* marked */       1)                                  "
      args:@[
        fileId,
        [info objectForKey:@"collectionId"],
        [info objectForKey:@"filename"],
        [info objectForKey:@"serverDocEJSON"],
        [info objectForKey:@"partition"]]
      readOnly:NO];
    if (isError(r2))
      return r2;
    return @{ @"action": @"new" };
  } else if (rows.count == 1) {
    // Have local file and have server doc, update server doc,
    // unless deleted locally.
    NSDictionary *r3 = [self
      sql:@" update file set serverDoc=?, onServer=1, marked=1 \
               where fileId=? and deleted=0                    "
      args:@[
        [info objectForKey:@"serverDocEJSON"],
        fileId]
      readOnly:NO];
    if (isError(r3))
      return r3;
    if ([[r3 objectForKey:@"rowsAffected"] integerValue] == 1)
      return @{ @"action": @"update" };
    else
      return @{};
  } else {
    return @{ @"error": @"multiple rows returned in mergeServerDoc" };
  }
}

-(NSDictionary *)setNotOnServer:(NSString *)fileId
{
  return
    [self
      sql:@" update file set onServer=0 where fileId=? "
      args:@[fileId]
      readOnly:NO];
}

// when a server doc exits the subscription or loses its
// filename field, mark onServer false
//
// we want to delete the file (and the database entry) if
// the file is a download or has delete set
//
// keep a non-deleted upload that hasn't completed its handshake
//
//
-(NSDictionary *)unmergeServerDoc:(NSString *)fileId
{
  NSDictionary *r1 =
    [self
      sql:@" select fileId, filename, onClient, deleted, original from file where fileId=? "
      args:@[fileId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  NSArray *rows = [r1 objectForKey:@"rows"];
  if (rows.count == 0) {
    // no local file and no server file, do nothing
    return @{};
  } else if (rows.count == 1) {
    // Have local file but server doc removed: remove if download or
    // deleted locally, but keep non-deleted upload that hasn't
    // completed its handshake.

    NSDictionary *row = [rows objectAtIndex:0];
    NSString *filename = [row objectForKey:@"filename"];
    NSNumber *onClient = [Data asBool:[row objectForKey:@"onClient"]];
    BOOL deleted = ((NSNumber *)[row objectForKey:@"deleted"]).boolValue;
    BOOL original = ((NSNumber *)[row objectForKey:@"original"]).boolValue;

    if (deleted || ! original) {
      NSDictionary *r2 = [self
        sql:@" delete from file where fileId=? "
        args:@[fileId]
        readOnly:NO];
      if (isError(r2))
        return r2;
      if ([[r2 objectForKey:@"rowsAffected"] integerValue] == 1)
        return @{
          @"action": @"remove",
          @"filename": filename,
          @"onClient": onClient
        };
      else
        return @{};
    } else {
      NSDictionary *r3 =
        [self
          sql:@" update file set onServer=0 where fileId=? "
          args:@[fileId]
          readOnly:NO];
      if (isError(r3))
        return r3;
      return @{};
    }
  } else {
    return @{@"error": @"multiple rows returned in unmergeServerDoc"};
  }
}

-(NSDictionary *)
beginMark:(NSString *)collectionId
{
  return
    [self
      sql:@" update file set marked=0 where collectionId=? "
      args:@[collectionId]
      readOnly:NO];
}

-(NSDictionary *)
deleteUnmarked:(NSString *)collectionId
{
  NSDictionary *r1 =
    [self
      sql:@" select fileId from file                 \
               where collectionId=? and              \
                     not marked and not original and \
                     not deleted                     "
      args:@[collectionId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  NSArray *removed =
    Underscore.pluck([r1 objectForKey:@"rows"], @"fileId");

  NSDictionary *r2 =
    [self
      sql:@" delete from file                        \
               where collectionId=? and              \
                     not marked and not original and \
                     not deleted                     "
      args:@[collectionId]
      readOnly:NO];
  if (isError(r2))
    return r2;

  return @{
    @"removed": removed
  };
}

-(NSDictionary *)
markFileAsDeleted:(NSString *)fileId
{
  NSDictionary *r1 =
    [self
      sql:@" update file set deleted=1 where fileId=? "
      args:@[fileId]
      readOnly:NO];
  if (isError(r1))
    return r1;
  return @{};
}

-(NSDictionary *)
removeDeletedFile:(NSString *)fileId
{
  NSDictionary *r1 =
    [self
      sql:@" delete from file where fileId=? "
      args:@[fileId]
      readOnly:NO];
  if (isError(r1))
    return r1;
  return @{};
}

-(NSDictionary *)
readDeletedFiles:(NSString *)collectionId
{
  NSDictionary *r1 =
    [self
      sql:@" select fileId from file where collectionId=? and deleted "
      args:@[collectionId]
      readOnly:YES];
  if (isError(r1))
    return r1;

  return @{
    @"fileIds": Underscore.pluck([r1 objectForKey:@"rows"], @"fileId")
  };
}

@end
