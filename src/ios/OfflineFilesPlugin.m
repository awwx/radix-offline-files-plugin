#import "OfflineFilesPlugin.h"
#import "Data.h"
#import "Queue.h"

#import "lib/Underscore/Underscore.h"

static BOOL debug = NO;

@interface OfflineFilesPlugin ()

@property NSString *eventCallbackId;

@property NSURLSession *session;

@property void (^backgroundEventsCompletionHandler)();

@property Data *data;

@property NSMutableArray *savedEvents;

@property NSNumber *started;

@property Queue *serializedQueue;

@end

@implementation OfflineFilesPlugin

- (void)pluginInitialize
{
  if (debug)
    NSLog(@"pluginInitialize");

  self.started = @(NO);

  [self createStagingDir];

  self.serializedQueue = [Queue new];

  self.savedEvents = [NSMutableArray arrayWithCapacity:10];

  self.data = [Data create:^(NSDictionary *event) {
    [self emitEvent:event];
  }];

  [self
    name:@"startup"
    backgroundReportingError:^(Callback callback) {
      callback([self.data startup]);
    }
    successCallback:^(NSDictionary *result) {
      if (debug)
        NSLog(@"SQL started");
      self.started = @(YES);
      [self emitEvent:@{@"event": @"started"}];
    }];

  self.session = [self backgroundSession];

  UIUserNotificationType types =
    UIUserNotificationTypeBadge |
    UIUserNotificationTypeSound |
    UIUserNotificationTypeAlert;

  UIUserNotificationSettings *mySettings =
    [UIUserNotificationSettings settingsForTypes:types categories:nil];

  [[UIApplication sharedApplication] registerUserNotificationSettings:mySettings];
}

- (void)
name:(NSString *)name
background:(Operation)operation
callback:(Callback)callback
{
  [self.serializedQueue name:name run:operation callback:callback];
}

- (void)
emitError:(NSString *)error
{
  [self emitEvent:@{
    @"event": @"error",
    @"error": error
  }];
}

- (void)
emitNSError:(NSError *)nsError
{
  [self emitError:[nsError localizedDescription]];
}

- (void)
emitErrorResult:(NSDictionary *)result
{
  [self emitError:[result objectForKey:@"error"]];
}

- (void)
name:(NSString *)name
backgroundReportingError:(Operation)operation
successCallback:(Callback)callback
{
  [self
    name:name
    background:^(Callback callback){
      operation(callback);
    }
    callback:^(NSDictionary *result) {
      if ([Data isError:result]) {
        [self emitErrorResult:result];
      } else {
        callback(@{});
      }
    }];
}

- (void)application:(UIApplication *)application
  didReceiveLocalNotification:(UILocalNotification *)notification
{
  [self emitEvent:@{
    @"event": @"notification",
    @"collectionId": [notification.userInfo objectForKey:@"collectionId"],
    @"notification": [notification.userInfo objectForKey:@"notification"]
  }];
}

- (NSString *)uniqueId
{
  return [[NSUUID UUID] UUIDString];
}

- (void)sendEvent:(NSDictionary*)event
{
  CDVPluginResult *result =
    [CDVPluginResult
      resultWithStatus:CDVCommandStatus_OK
      messageAsDictionary: event
    ];
  [result setKeepCallbackAsBool:YES];
  [self.commandDelegate sendPluginResult:result callbackId:self.eventCallbackId];
}

-(void)emitEvent:(NSDictionary *)event
{
  if (self.eventCallbackId) {
    [self sendEvent:event];
  } else {
    [self.savedEvents addObject:event];
    if ([event objectForKey:@"error"])
      NSLog(@"OfflineFilePlugin: %@", event);
  }
}

- (void)reportError:(NSError *)error
forContext:(NSString *)context
{
  NSMutableDictionary *event =
    [NSMutableDictionary dictionaryWithObjectsAndKeys:
      @"nserror",                              @"event",
      [NSNumber numberWithInteger:error.code], @"code",
      error.domain,                            @"domain",
      error.description,                       @"description",
      nil];
  if (context) {
    [event setObject:context forKey:@"context"];
  }

  [self emitEvent:event];
}

- (void)emitSavedEvents
{
  for (NSDictionary *event in self.savedEvents) {
    [self sendEvent:event];
  }
  self.savedEvents = nil;
}

- (void)dumpTasks0
{
  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      NSLog(@"========= uploadTasks");
      for (NSURLSessionUploadTask *task in uploadTasks) {
        NSLog(@"%@", [OfflineFilesPlugin taskInfo:task type:@"upload"]);
      }
      NSLog(@"========= downloadTasks: %@", downloadTasks);
    }
  ];
}

- (void)onReset
{
}

+ (NSString*)taskState:(NSURLSessionTask*)task
{
  switch (task.state) {
    case NSURLSessionTaskStateRunning:   return @"running";
    case NSURLSessionTaskStateSuspended: return @"suspended";
    case NSURLSessionTaskStateCanceling: return @"canceling";
    case NSURLSessionTaskStateCompleted: return @"completed";
    default:                             return @"unknown";
  }
}

+ (NSDictionary*)taskInfo:(NSURLSessionTask*)task
type:(NSString*)type
{
  return [NSDictionary dictionaryWithObjectsAndKeys:
           type, @"type",
           [OfflineFilesPlugin taskState:task], @"state",
           task.originalRequest.URL.absoluteString, @"url",
           nil
         ];
}

-(void)listen:(CDVInvokedUrlCommand*)command
{
  self.eventCallbackId = command.callbackId;

  [self emitSavedEvents];

  if ([self.started boolValue]) {
    [self emitEvent:@{@"event": @"started"}];
  }
}

- (void)dumpTasks:(CDVInvokedUrlCommand*)command
{
  @try {
    [self dumpTasks0];
  }
  @catch (NSException *exception) {
    [self.commandDelegate
       sendPluginResult:
         [CDVPluginResult
           resultWithStatus:CDVCommandStatus_ERROR
           messageAsDictionary:
             [NSDictionary dictionaryWithObjectsAndKeys:
               exception.name, @"name",
               exception.reason, @"reason",
               exception.callStackSymbols, @"stack",
               nil
             ]
         ]
       callbackId:command.callbackId
    ];

    return;
  }

  [self.commandDelegate
     sendPluginResult:
       [CDVPluginResult
         resultWithStatus:CDVCommandStatus_OK
         messageAsString:@"OK"]
     callbackId:command.callbackId
  ];
}


//
// AppDelegate
//

- (void)application:(UIApplication *)application
handleEventsForBackgroundURLSession:(NSString *)identifier
completionHandler:(void (^)(void))completionHandler
{
  if (debug)
    NSLog(@"handleEventsForBackgroundURLSession");

  self.backgroundEventsCompletionHandler = completionHandler;
}


//
// NSURLSessionDelegate
//

- (void)URLSession:(NSURLSession *)session
didBecomeInvalidWithError:(NSError *)error
{
  [self reportError:error forContext:@"session became invalid with error"];
}

// URLSession:didReceiveChallenge:completionHandler:
// as this is not implemented, the session calls
// `URLSession:task:didReceiveChallenge:completionHandler:` instead.

- (void)URLSessionDidFinishEventsForBackgroundURLSession:(NSURLSession *)session
{
  if (debug)
    NSLog(@"URLSessionDidFinishEventsForBackgroundURLSession");

  [[NSOperationQueue mainQueue] addOperationWithBlock:^{
    if (self.backgroundEventsCompletionHandler)
      self.backgroundEventsCompletionHandler();
    self.backgroundEventsCompletionHandler = nil;
  }];
}


+(NSString *)
task:(NSURLSessionTask *)task
queryField:(NSString *)name
{
  NSURLQueryItem *found =
    Underscore.find(
      [NSURLComponents
        componentsWithURL:task.originalRequest.URL
        resolvingAgainstBaseURL:NO].queryItems,
      ^BOOL(NSURLQueryItem *item) {
        return [name isEqual:item.name];
      }
    );

  if (found)
    return found.value;
  else
    return nil;
}

-(NSString *)collectionIdOfUploadTask:(NSURLSessionUploadTask *)task
{
  NSString *collectionId =
    [OfflineFilesPlugin task:task queryField:@"collection"];

  if (! collectionId)
    [self emitError:@"collectionId not found in upload task query string"];

  return collectionId;
}

-(NSString *)fileIdOfUploadTask:(NSURLSessionUploadTask *)task
{
  NSString * fileId =
    [OfflineFilesPlugin task:task queryField:@"file"];

  if (! fileId)
    [self emitError:@"fileId not found in upload task query string"];

  return fileId;
}

+(NSString *)collectionIdOfDownloadTask:(NSURLSessionDownloadTask *)task
{
  NSArray *cs = task.originalRequest.URL.pathComponents;
  return [cs objectAtIndex:cs.count - 2];
}

+(NSString *)fileIdOfDownloadTask:(NSURLSessionDownloadTask *)task
{
  NSArray *cs = task.originalRequest.URL.pathComponents;
  return [cs objectAtIndex:cs.count - 1];
}


//
// NSURLSessionTaskDelegate methods
//

- (void)URLSession:(NSURLSession *)session
task:(NSURLSessionTask *)task
didCompleteWithError:(NSError *)error
{
  if (debug)
    NSLog(@"file transfer task completed");

  if (error != nil) {
    if (! (error.code == NSURLErrorCancelled ||
           error.code == ECANCELED ||
           // "Lost connection to background transfer service"
           error.code == -997 ||
           error.code == NSURLErrorCannotParseResponse ||
           error.code == NSURLErrorUnknown))
    {
      [self reportError:error forContext:@"transfer task completed"];
    }
    return;
  }

  NSHTTPURLResponse *response = (NSHTTPURLResponse *)task.response;

  if (response.statusCode != 200) {
    // TODO user error
    [self emitError:[NSString stringWithFormat:
      @"transfer failed for %@ with status code %ld",
      task.originalRequest.URL,
      (long)response.statusCode]];
    return;
  }

  [self
    name:@"transfer task completed"
    background:^(Callback callback) {
      if ([task isKindOfClass:[NSURLSessionUploadTask class]]) {
        NSURLSessionUploadTask *uploadTask = (NSURLSessionUploadTask *)task;
        NSString *collectionId =
          [self collectionIdOfUploadTask:uploadTask];
        NSString *fileId =
          [self fileIdOfUploadTask:uploadTask];

        if (! collectionId) {
          [self emitError:@"collection not found in task url query string"];
          return;
        }

        if (! fileId) {
          [self emitError:@"file not found in task url query string"];
          return;
        }

        NSDictionary *uploadCompleteResult =
          [self.data uploadComplete:collectionId fileId:fileId];
        if ([Data isError:uploadCompleteResult]) {
          callback(uploadCompleteResult);
          return;
        }

        NSDictionary *taskCompleteResult =
          [self.data fileTasksComplete:fileId];
        if ([Data isError:taskCompleteResult]) {
          callback(taskCompleteResult);
          return;
        }

        NSDictionary *jobsDoneResult = [self.data jobsDone];
        if ([Data isError:jobsDoneResult]) {
          callback(jobsDoneResult);
          return;
        }

        for (NSDictionary *row in [jobsDoneResult objectForKey:@"rows"]) {
          NSObject *notification = [row objectForKey:@"notification"];
          if ([notification isKindOfClass:[NSString class]]) {
            [self collection:collectionId notify:(NSString *)notification];
          }
          NSDictionary *removeJobResult =
            [self.data removeJob:[row objectForKey:@"jobId"]];
          if ([Data isError:removeJobResult]) {
            callback(removeJobResult);
            return;
          }
        }
      } else if ([task isKindOfClass:[NSURLSessionDownloadTask class]]) {
        // do nothing, handled by downloadTask:didFinishDownloadingToURL
      }
      callback([Data okResult]);
    }
    callback:^(NSDictionary *result) {
      if ([Data isError:result]) {
        [self emitError:[result objectForKey:@"error"]];
      }
    }];
}

- (void)URLSession:(NSURLSession *)session
task:(NSURLSessionTask *)task
didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge
completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition,
                            NSURLCredential *credential))completionHandler
{
  completionHandler(NSURLSessionAuthChallengeCancelAuthenticationChallenge, nil);
}


- (void)URLSession:(NSURLSession *)session
task:(NSURLSessionTask *)task
didSendBodyData:(int64_t)bytesSent
totalBytesSent:(int64_t)totalBytesSent
totalBytesExpectedToSend:(int64_t)totalBytesExpectedToSend
{
}


//
// NSURLSessionDownloadDelegate
//

+(NSURL *)noCloudDir
{
  return
    [[NSURL
       fileURLWithPath:
         [NSSearchPathForDirectoriesInDomains(
            NSLibraryDirectory, NSUserDomainMask, YES)
           objectAtIndex: 0]
       isDirectory:YES]
      URLByAppendingPathComponent:@"NoCLoud/"];
}

+(NSURL *)stagingDir
{
  return
    [[OfflineFilesPlugin noCloudDir]
       URLByAppendingPathComponent:@"offline-files/staging/"];
}

-(void)createStagingDir
{
  NSError *error = nil;

  [[NSFileManager defaultManager]
    createDirectoryAtURL:[OfflineFilesPlugin stagingDir]
    withIntermediateDirectories:YES
    attributes:nil
    error:&error];

  if (error) {
    [self reportError:error forContext:@"createStagingDir"];
  }
}

-(BOOL)
moveFile:(NSURL *)location
toURL:(NSURL *)destination
{
  NSError *moveError;
  if (! [[NSFileManager defaultManager]
          moveItemAtURL:location
          toURL:destination
          error:&moveError])
  {
    // TODO context
    if (moveError)
      [self emitNSError:moveError];
    else
      [self emitError:@"file move not successful, no NSError"];
    return NO;
  }

  return YES;
}

- (void)URLSession:(NSURLSession *)session
downloadTask:(NSURLSessionDownloadTask *)downloadTask
didFinishDownloadingToURL:(NSURL *)location
{
  if (debug)
    NSLog(@"download task finished");

  NSHTTPURLResponse *response = (NSHTTPURLResponse *)downloadTask.response;

  NSString *fileId = [OfflineFilesPlugin fileIdOfDownloadTask:downloadTask];

  bool successful = YES;

  if (response.statusCode != 200) {
    [self emitError:
      [NSString stringWithFormat:
        @"download failed with status code: %ld", (long) response.statusCode]];
    successful = NO;
  }

  NSURL *stagingPath =
    [[OfflineFilesPlugin stagingDir]
      URLByAppendingPathComponent:fileId];

  if (successful) {
    if (! [self moveFile:location toURL:stagingPath])
      return;
  }

  [self
    name:@"download finished"
    backgroundReportingError:^() {
      void (^removeStaging)(void) = ^{
        NSError *error;
        if (! [[NSFileManager defaultManager]
                 removeItemAtURL:stagingPath
                 error:&error])
        {
          [self reportError:error forContext:@"remove downloaded staging file"];
        }
      };

      NSDictionary *r1 = [self.data readFileWithCollectionPath:fileId];
      if ([Data isError:r1]) {
        removeStaging();
        return r1;
      }

      NSDictionary *file = [r1 objectForKey:@"file"];

      if (! file) {
        removeStaging();
        return @{
          @"error":
            [NSString stringWithFormat:
              @"downloaded file not found in database: %@", fileId]
        };
      }

      if ([[file objectForKey:@"deleted"] boolValue]) {
        removeStaging();
        return @{};
      }

      NSString *collectionId   = [file objectForKey:@"collectionId"];
      NSString *collectionPath = [file objectForKey:@"collectionPath"];
      NSString *filename       = [file objectForKey:@"filename"];

      NSURL *dstUrl =
        [NSURL
          URLWithString:
            [collectionPath stringByAppendingString:filename]];

      if (! [self moveFile:stagingPath toURL:dstUrl]) {
        // error has already been reported
        removeStaging();
        return @{};
      }

      NSDictionary *r2 = [self.data downloadComplete:fileId];
      if ([Data isError:r2])
        return r2;

      [self emitEvent: @{
        @"event": @"updated",
        @"collectionId": collectionId,
        @"fileId": fileId
      }];

      return @{};
    }];
}

- (NSURLSession *)backgroundSession
{
  static NSURLSession *session = nil;
  static dispatch_once_t onceToken;
  dispatch_once(&onceToken, ^{
    NSURLSessionConfiguration *configuration =
     [NSURLSessionConfiguration
       backgroundSessionConfigurationWithIdentifier:@"offline-files"];

    configuration.sessionSendsLaunchEvents = NO;

    session = [NSURLSession
                sessionWithConfiguration:configuration
                delegate:self
                delegateQueue:nil];
  });

  return session;
}

// for testing
- (void)quit:(CDVInvokedUrlCommand*)command
{
  @try {
    NSLog(@"========== quit");
    exit(0);
  }
  @catch (NSException *exception) {
    [self.commandDelegate
       sendPluginResult:
         [CDVPluginResult
           resultWithStatus:CDVCommandStatus_ERROR
           messageAsDictionary:
             [NSDictionary dictionaryWithObjectsAndKeys:
               exception.name, @"name",
               exception.reason, @"reason",
               exception.callStackSymbols, @"stack",
               nil
             ]
         ]
       callbackId:command.callbackId
    ];

    return;
  }

  [self.commandDelegate
     sendPluginResult:
       [CDVPluginResult
         resultWithStatus:CDVCommandStatus_OK
         messageAsString:@"quit:OK"]
     callbackId:command.callbackId
  ];
}

- (void)onCommand:(CDVInvokedUrlCommand*)command
catchErr:(void (^)(void))f
{
  @try {
    f();
  }
  @catch (NSException *exception) {
    CDVPluginResult *result =
      [CDVPluginResult
        resultWithStatus:CDVCommandStatus_ERROR
        messageAsDictionary:
          [NSDictionary dictionaryWithObjectsAndKeys:
            exception.name, @"name",
            exception.reason, @"reason",
            exception.callStackSymbols, @"stack",
            nil
          ]
      ];
    [result setKeepCallbackAsBool:YES];
    [self.commandDelegate
       sendPluginResult:result
       callbackId:command.callbackId
    ];
  }
}

- (void)genErr:(CDVInvokedUrlCommand*)command
{
  [self onCommand:command catchErr:^{
    [NSException raise:@"Invalid foo value" format:@"foo is invalid"];
  }];

  [self.commandDelegate
     sendPluginResult:
       [CDVPluginResult
         resultWithStatus:CDVCommandStatus_OK
         messageAsString:@"OK"]
     callbackId:command.callbackId
  ];
}

- (void)respondOK:(CDVInvokedUrlCommand*)command
{
  [self.commandDelegate
    sendPluginResult:
      [CDVPluginResult
        resultWithStatus:CDVCommandStatus_OK
        messageAsBool:YES]
    callbackId:command.callbackId];
}

-(void)
collection:(NSString *)collectionId
notify:(NSString *)notificationJSON
{
  if (debug)
    NSLog(@"upload job complete");

  UILocalNotification *n = [[UILocalNotification alloc] init];

  NSDictionary *r1 = [Data deserialize:notificationJSON];
  if ([Data isError:r1]) {
    [self emitErrorResult:r1];
    return;
  }

  NSObject *d = [r1 objectForKey:@"value"];
  if (! [d isKindOfClass:[NSDictionary class]])
    return;

  NSDictionary *notification = (NSDictionary *)d;

  // TODO more fields

  NSString *alertBody = [notification objectForKey:@"alertBody"];
  if (alertBody)
    n.alertBody = alertBody;

  NSString *soundName = [notification objectForKey:@"soundName"];
  if (soundName) {
    if ([soundName isEqual:@":default:"])
      n.soundName = UILocalNotificationDefaultSoundName;
    else
      n.soundName = soundName;
  }

  n.userInfo = @{
    @"collectionId": collectionId,
    @"notification": notificationJSON
  };

  [[UIApplication sharedApplication] presentLocalNotificationNow:n];
}

-(void)configure:(CDVInvokedUrlCommand*)command
{
  NSDictionary *configuration = [command.arguments objectAtIndex:0];

  [self
    name:@"configure"
    command:command
    bgResult:^() {
      return [self.data configure:configuration];
    }];
}

-(void)configureCollection:(CDVInvokedUrlCommand*)command
{
  NSDictionary *collectionConfiguration = [command.arguments objectAtIndex:0];

  [self
    name:@"configureCollection"
    command:command
    bgResult:^() {
      return [self.data configureCollection:collectionConfiguration];
    }];
}

-(void)
name:(NSString *)name
command:(CDVInvokedUrlCommand *)command
background:(Operation)operation
{
  [self
    name:name
    background:operation
    callback:^(NSDictionary *result){
      [self respond:command result:result];
    }];
}

-(void)
name:(NSString *)name
backgroundReportingError:(NSDictionary * (^)(void))thunk
{
  [self
    name:name
    background:^(Callback callback) {
      callback(thunk());
    }
    callback:^(NSDictionary *result) {
      if ([Data isError:result])
        [self emitErrorResult:result];
    }];
}

-(void)
name:(NSString *)name
command:(CDVInvokedUrlCommand*)command
bgResult:(NSDictionary *(^)())operation
{
  [self
    name:name
    command:command
    background:^(Callback callback) {
      callback(operation());
    }];
}

-(void)addOriginalFile:(CDVInvokedUrlCommand*)command
{
  NSDictionary *file = [command.arguments objectAtIndex:0];

  [self
    name:@"addOriginalFile"
    command:command
    background:^(Callback callback){
      NSDictionary *r1 = [self.data addOriginal:file];
      if ([Data isError:r1]) {
        callback(r1);
        return;
      }

      NSString *collectionId = [file objectForKey:@"collectionId"];
      NSDictionary *r2 = [self.data readAutoUpload:collectionId];
      if ([Data isError:r2]) {
        callback(r2);
        return;
      }

      NSNumber *autoUpload = [r2 objectForKey:@"autoUpload"];
      if (autoUpload.boolValue) {
        NSDictionary *r3 = [self.data readConfig:@"uploadUrl"];
        if ([Data isError:r3]) {
          callback(r3);
          return;
        }
        NSString *uploadUrl = [r3 objectForKey:@"value"];

        NSDictionary *r4 =
          [self.data readFileWithCollectionPath:[file objectForKey:@"fileId"]];
        if ([Data isError:r4]) {
          callback(r4);
          return;
        }

        [self addUploadTask:uploadUrl file:[r4 objectForKey:@"file"]];
      }
      callback([Data okResult]);
    }];
}

-(void)
respond:(CDVInvokedUrlCommand*)command
withError:(NSDictionary *)error
{
  [self.commandDelegate
    sendPluginResult:
      [CDVPluginResult
        resultWithStatus:CDVCommandStatus_ERROR
        messageAsString:[error objectForKey:@"error"]]
    callbackId:command.callbackId];
}

-(void)respond:(CDVInvokedUrlCommand*)command
result:(NSDictionary *)result
{
  if ([Data isError:result]) {
    [self respond:command withError:result];
  } else {
    [self.commandDelegate
      sendPluginResult:
        [CDVPluginResult
          resultWithStatus:CDVCommandStatus_OK
          messageAsDictionary:result]
      callbackId:command.callbackId];
  }
}

-(void)setAutoUploadOn:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  // First get upload tasks already running, and then enter the
  // serial queue.

  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      // We're in the default NSURLSession delegate queue

      NSArray *excludeFileIds =
        [self listUploadingFiles:collectionId uploadTasks:uploadTasks];

      // Now enter the serial queue

      [self
        name:@"setAutoUploadOn"
        command:command
        bgResult:^() {
          NSDictionary *r1 = [self.data switchToAutoUploadOn:collectionId];
          if ([Data isError:r1])
            return r1;

          NSDictionary *r2 =
            [self.data
                readAllFilesNeedingUpload:collectionId
                excluding:excludeFileIds];
          if ([Data isError:r2])
            return r2;
          NSArray *files = [r2 objectForKey:@"files"];

          NSDictionary *r3 = [self.data readConfig:@"uploadUrl"];
          if ([Data isError:r3])
            return r3;
          NSString *uploadUrl = [r3 objectForKey:@"value"];

          for (NSDictionary *file in files) {
            [self addUploadTask:uploadUrl file:file];
          }

          return @{};
        }];
    }];
}

-(void)setAutoUploadOff:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  [self
    name:@"setAutoUploadOff"
    command:command
    background:^(Callback callback) {
      NSDictionary *r1 =
        [self.data switchToAutoUploadOff:collectionId];
      if ([Data isError:r1]) {
        callback(r1);
        return;
      }

      [self.session getTasksWithCompletionHandler:
        ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
          for (NSURLSessionUploadTask *uploadTask in uploadTasks) {
            [uploadTask cancel];
          }
          callback(@{});
        }];
    }];
}

-(void)cancelUpload:(CDVInvokedUrlCommand*)command
{
  NSString *fileId = [command.arguments objectAtIndex:0];

  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      for (NSURLSessionUploadTask *uploadTask in uploadTasks) {
        NSString *taskFileId =
          [self fileIdOfUploadTask:uploadTask];
        if (fileId && [fileId isEqual:taskFileId]) {
          [uploadTask cancel];
          break;
        }
      }
      [self respond:command result:@{}];
    }];
}

-(void)readFile:(CDVInvokedUrlCommand*)command
{
  NSString *fileId = [command.arguments objectAtIndex:0];

  [self
    name:@"readFile"
    command:command
    bgResult:^() {
      return [self.data readFile:fileId];
    }];
}

-(void)readFilesForPartition:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];
  NSString *partition    = [command.arguments objectAtIndex:1];

  [self
    name:@"readFilesForPartition"
    command:command
    bgResult:^() {
      return [self.data
               readFilesForPartition:collectionId
               partition:partition];
    }];
}


-(void)
addUploadTasks:(NSDictionary *)result
andRespond:(CDVInvokedUrlCommand*)command
{
  if ([Data isError:result]) {
    [self respond:command withError:result];
    return;
  }

  NSArray *files = [result objectForKey:@"files"];
  if (files.count == 0) {
    [self respond:command result:@{}];
    return;
  }

  NSString *uploadUrl = [result objectForKey:@"uploadUrl"];

  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      NSMutableDictionary *taskByFileId =
        [NSMutableDictionary dictionaryWithCapacity:uploadTasks.count];
      for (NSURLSessionUploadTask *task in uploadTasks) {
        NSString *taskFileId =
          [self fileIdOfUploadTask:task];
        if (taskFileId)
          [taskByFileId setObject:task forKey:taskFileId];
      }
      for (NSDictionary *file in files) {
        if (! [taskByFileId objectForKey:[file objectForKey:@"fileId"]]) {
          [self addUploadTask:uploadUrl file:file];
       }
     }
     [self respond:command result:@{}];
   }];
}

-(void)uploadAll:(CDVInvokedUrlCommand*)command
{
  NSString *jobId  =       [command.arguments objectAtIndex:0];
  NSString *collectionId = [command.arguments objectAtIndex:1];
  NSObject *notification = [command.arguments objectAtIndex:2];

  NSDictionary *r0 = [Data serialize:notification];
  if ([Data isError:r0]) {
    [self respond:command withError:r0];
    return;
  }
  NSString *notificationJSON = [r0 objectForKey:@"value"];

  [self
    name:@"uploadAll"
    background:^(Callback callback) {
      // If auto upload is on, do nothing

      NSDictionary *r1 = [self.data readAutoUpload:collectionId];
      if ([Data isError:r1]) {
        callback(r1);
        return;
      }

      if (((NSNumber *)[r1 objectForKey:@"autoUpload"]).boolValue) {
        callback([Data okResult]);
        return;
      }

      NSDictionary *r2 = [self.data readConfig:@"uploadUrl"];
      if ([Data isError:r2]) {
        callback(r2);
        return;
      }

      NSString *uploadUrl = [r2 objectForKey:@"value"];

      NSDictionary *r3 =
        [self.data
          uploadAll:jobId
          collection:collectionId
          notification:notificationJSON];
      if ([Data isError:r3]) {
        callback(r3);
        return;
      }

      callback(@{
        @"uploadUrl": uploadUrl,
        @"files": [r3 objectForKey:@"files"]
      });
    }
    callback:^(NSDictionary *result) {
      [self addUploadTasks:result andRespond:command];
    }];
}

-(void)uploadPartition:(CDVInvokedUrlCommand*)command
{
  NSString *jobId  =       [command.arguments objectAtIndex:0];
  NSString *collectionId = [command.arguments objectAtIndex:1];
  NSString *partition    = [command.arguments objectAtIndex:2];
  NSObject *notification = [command.arguments objectAtIndex:3];

  NSDictionary *r0 = [Data serialize:notification];
  if ([Data isError:r0]) {
    [self respond:command withError:r0];
    return;
  }
  NSString *notificationJSON = [r0 objectForKey:@"value"];

  [self
    name:@"uploadPartition"
    background:^(Callback callback) {
      // If auto upload is on, do nothing

      NSDictionary *r1 = [self.data readAutoUpload:collectionId];
      if ([Data isError:r1]) {
        callback(r1);
        return;
      }

      if (((NSNumber *)[r1 objectForKey:@"autoUpload"]).boolValue) {
        callback([Data okResult]);
        return;
      }

      NSDictionary *r2 = [self.data readConfig:@"uploadUrl"];
      if ([Data isError:r2]) {
        callback(r2);
        return;
      }
      NSString *uploadUrl = [r2 objectForKey:@"value"];

      NSDictionary *r3 =
        [self.data
          uploadPartition:jobId
          collection:collectionId
          partition:partition
          notification:notificationJSON];
      if ([Data isError:r3]) {
        callback(r3);
        return;
      }

      callback(@{
        @"uploadUrl": uploadUrl,
        @"files": [r3 objectForKey:@"files"]
      });
    }
    callback:^(NSDictionary *result) {
      [self addUploadTasks:result andRespond:command];
    }];
}

-(void)convertToDownload:(CDVInvokedUrlCommand*)command
{
  NSString *fileId = [command.arguments objectAtIndex:0];

  [self
    name:@"convertToDownload"
    command:command
    bgResult:^() {
      NSDictionary *result = [self.data convertToDownload:fileId];
      if ([Data isError:result])
        return result;
      else
        return @{};
    }];
}

-(void)download:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];
  NSString *fileId       = [command.arguments objectAtIndex:1];

  // first get tasks, and then enter the serial queue

  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      // We're in the default NSURLSession delegate queue

      if (Underscore.find(
            downloadTasks,
            ^BOOL(NSURLSessionDownloadTask* task) {
              NSString *taskCollectionId =
                [OfflineFilesPlugin collectionIdOfDownloadTask:task];
              NSString *taskFileId =
                [OfflineFilesPlugin fileIdOfDownloadTask:task];
              if (! taskCollectionId || ! taskFileId)
                return NO;
              return ([collectionId isEqual:taskCollectionId] &&
                      [fileId isEqual:taskFileId]);
            }))
      {
        [self respond:command result:@{ @"result": @"already-downloading" }];
        return;
      }

      // Now enter the serial queue

      [self
        name:@"download"
        command:command
        bgResult:^() {
          NSDictionary *r1 =
            [self.data download:fileId];
          if ([Data isError:r1])
            return r1;

          return [self addDownloadTask:collectionId fileId:fileId];
        }];
  }];
}


// Return a list of fileId's for download tasks in the collection.

-(NSArray *)
listDownloadingFiles:(NSString *)collectionId
downloadTasks:(NSArray *)downloadTasks
{
  return
    Underscore.arrayMap(
      downloadTasks,
      ^NSString *(NSURLSessionDownloadTask* task) {
        NSString *taskCollectionId =
          [OfflineFilesPlugin collectionIdOfDownloadTask:task];
        NSString *taskFileId =
          [OfflineFilesPlugin fileIdOfDownloadTask:task];
        if (! taskCollectionId || ! taskFileId)
          return nil;
        if ([collectionId isEqual:taskCollectionId])
          return taskFileId;
        else
          return nil;
      }
    );
}

-(NSArray *)
listUploadingFiles:(NSString *)collectionId
uploadTasks:(NSArray *)uploadTasks
{
  return
    Underscore.arrayMap(
      uploadTasks,
      ^NSString *(NSURLSessionUploadTask *task) {
        NSString *taskCollectionId =
          [self collectionIdOfUploadTask:task];
        NSString *taskFileId =
          [self fileIdOfUploadTask:task];
        if (! taskCollectionId || ! taskFileId)
          return nil;
        if ([collectionId isEqual:taskCollectionId])
          return taskFileId;
        else
          return nil;
      });
}

// TODO this would be more efficient if each collection had its own
// NSURLSession.

-(void)downloadAll:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  // First get download tasks already running, and then enter the
  // serial queue.

  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      // We're in the default NSURLSession delegate queue

      NSArray *excludeFileIds =
        [self listDownloadingFiles:collectionId downloadTasks:downloadTasks];

      // Now enter the serial queue

      [self
        name:@"downloadAll"
        command:command
        bgResult:^() {
          NSDictionary *r1 =
            [self.data downloadAll:collectionId excluding:excludeFileIds];
          if ([Data isError:r1])
            return r1;

          NSArray *fileIds = [r1 objectForKey:@"fileIds"];

          return
            [self
              addDownloadTasks:collectionId
              fileIds:fileIds];
        }];
  }];
}

-(void)resumeTransfers:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  [self.session getTasksWithCompletionHandler:
    ^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
      // We're in the default NSURLSession delegate queue

      NSArray *excludeDownloads =
        [self listDownloadingFiles:collectionId downloadTasks:downloadTasks];

      NSArray *excludeUploads =
        [self listUploadingFiles:collectionId uploadTasks:uploadTasks];

      // Now enter the serial queue

      [self
        name:@"resumeTransfers"
        command:command
        bgResult:^() {
          NSDictionary *r1 =
            [self.data readDownloads:collectionId excluding:excludeDownloads];
          if ([Data isError:r1])
            return r1;

          NSArray *fileIds = [r1 objectForKey:@"fileIds"];

          for (NSString *fileId in fileIds) {
            [self addDownloadTask:collectionId fileId:fileId];
          }

          NSDictionary *r2 =
            [self.data readUploads:collectionId excluding:excludeUploads];
          if ([Data isError:r2])
            return r2;
          NSArray *files = [r2 objectForKey:@"files"];

          NSDictionary *r3 = [self.data readConfig:@"uploadUrl"];
          if ([Data isError:r3])
            return r3;
          NSString *uploadUrl = [r3 objectForKey:@"value"];

          for (NSDictionary *file in files) {
            [self addUploadTask:uploadUrl file:file];
          }

          return @{};
        }];
    }];
}

-(void)
addUploadTask:(NSString *)uploadUrl
file:(NSDictionary *)file
{
  [self
    addUploadTask:uploadUrl
    collectionId:[file objectForKey:@"collectionId"]
    collectionPath:[file objectForKey:@"collectionPath"]
    fileId:[file objectForKey:@"fileId"]
    filename:[file objectForKey:@"filename"]
    serverDoc:[file objectForKey:@"serverDoc"]];
}

-(void)
addUploadTask:(NSString *)uploadUrl
collectionId:(NSString *)collectionId
collectionPath:(NSString *)collectionPath
fileId:(NSString *)fileId
filename:(NSString *)filename
serverDoc:(NSString *)serverDoc
{
  NSMutableCharacterSet *set =
    [[NSCharacterSet URLQueryAllowedCharacterSet] mutableCopy];
  [set removeCharactersInString:@"&+=?"];

  NSString *encodedCollection =
    [collectionId
      stringByAddingPercentEncodingWithAllowedCharacters:set];

  NSString *encodedFile =
    [fileId
      stringByAddingPercentEncodingWithAllowedCharacters:set];

  NSString *encodedDoc =
    [serverDoc
      stringByAddingPercentEncodingWithAllowedCharacters:set];

  NSURL *fileURL =
    [NSURL
      URLWithString:filename
      relativeToURL:[NSURL URLWithString:collectionPath]];

  NSURL *uploadURL =
    [NSURL URLWithString:
      [[[[[[uploadUrl stringByAppendingString:@"?collection="]
            stringByAppendingString:encodedCollection]
           stringByAppendingString:@"&file="]
          stringByAppendingString:encodedFile]
         stringByAppendingString:@"&doc="]
        stringByAppendingString:encodedDoc]];

  NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:uploadURL];
  request.HTTPMethod = @"PUT";

  NSURLSessionUploadTask *uploadTask =
    [self.session
      uploadTaskWithRequest:request
      fromFile:fileURL];

  [uploadTask resume];
}

-(NSDictionary *)
addDownloadTask:(NSString *)collectionId
fileId:(NSString *)fileId
{
  NSDictionary *r1 = [self.data readConfig:@"downloadUrl"];
  if ([Data isError:r1])
    return r1;

  NSURL *downloadURL =
    [NSURL URLWithString:
      [[[[[r1 objectForKey:@"value"]
           stringByAppendingString:@"/"]
          stringByAppendingString:collectionId]
         stringByAppendingString:@"/"]
        stringByAppendingString:fileId]];

  NSURLSessionDownloadTask *downloadTask =
    [self.session downloadTaskWithURL:downloadURL];

  [downloadTask resume];

  return @{};
}

-(NSDictionary *)
addDownloadTasks:(NSString *)collectionId
fileIds:(NSArray *)fileIds
{
  for (NSString *fileId in fileIds) {
    NSDictionary *r = [self addDownloadTask:collectionId fileId:fileId];
    if ([Data isError:r])
      return r;
  }
  return @{};
}

-(void)mergeServerDoc:(CDVInvokedUrlCommand*)command
{
  NSDictionary *fileInfo = [command.arguments objectAtIndex:0];

  [self
    name:@"mergeServerDoc"
    command:command
    bgResult:^(){
      return [self.data mergeServerDoc:fileInfo];
    }];
}

-(void)unmergeServerDoc:(CDVInvokedUrlCommand*)command
{
  NSString *fileId = [command.arguments objectAtIndex:0];

  [self
    name:@"unmergeServerDoc"
    command:command
    bgResult:^(){
      return [self.data unmergeServerDoc:fileId];
    }];
}

-(void)beginMark:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  [self
    name:@"beginMark"
    command:command
    bgResult:^(){
      return [self.data beginMark:collectionId];
    }];
}

-(void)deleteUnmarked:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  [self
    name:@"deleteUnmarked"
    command:command
    bgResult:^(){
      return [self.data deleteUnmarked:collectionId];
    }];
}

-(void)markFileAsDeleted:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];
  NSString *fileId       = [command.arguments objectAtIndex:1];

  [self
    name:@"markFileAsDeleted"
    command:command
    bgResult:^(){
      return [self.data markFileAsDeleted:collectionId fileId:fileId];
    }];
}

-(void)removeDeletedFile:(CDVInvokedUrlCommand*)command
{
  NSString *fileId = [command.arguments objectAtIndex:0];

  [self
    name:@"removeDeletedFile"
    command:command
    bgResult:^(){
      return [self.data removeDeletedFile:fileId];
    }];
}

-(void)readDeletedFiles:(CDVInvokedUrlCommand*)command
{
  NSString *collectionId = [command.arguments objectAtIndex:0];

  [self
    name:@"readDeletedFiles"
    command:command
    bgResult:^(){
      return [self.data readDeletedFiles:collectionId];
    }];
}

@end
