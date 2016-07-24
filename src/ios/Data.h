#import <Foundation/Foundation.h>

typedef void (^Callback)(NSDictionary *);

@interface Data : NSObject

+(BOOL)isError:(NSDictionary *)result;

+(NSDictionary *)okResult;
+(NSDictionary *)exceptionResult:(NSException *)exception;

+(NSString *)serialize:(NSObject *)obj;
+(NSObject *)deserialize:(NSString *)json;

-(NSDictionary *)readConfig:(NSString *)key;

-(NSDictionary *)startup;

-(NSDictionary *)configure:(NSDictionary *)configuration;

-(NSDictionary *)configureCollection:(NSDictionary *)collection;

-(NSDictionary *)
addUpload:(NSDictionary *)data;

-(NSDictionary *)
uploadAll:(NSString *)jobId
collection:(NSString *)collectionId
notification:(NSString *)notification;

-(NSDictionary *)
uploadPartition:(NSString *)jobId
collection:(NSString *)collectionId
partition:(NSString *)group
notification:(NSString *)notification;

-(NSDictionary *)download:(NSString *)fileId;

-(NSDictionary *)
allDownloads:(NSString *)collectionId
excluding:(NSArray *)excludingFileIds;

-(NSDictionary *)readAllFilesNeedingUpload:(NSString *)collectionId;

-(NSDictionary *)readDownloads;

-(NSDictionary *)readJobsOfFile:(NSString *)fileId;

-(NSDictionary *)fileTasksComplete:(NSString *)fileId;

-(NSDictionary *)jobsDone;

-(NSDictionary *)removeJob:(NSString *)jobId;

-(NSDictionary *)uploadComplete:(NSString *)fileId;

-(NSDictionary *)
setUpload:(NSString *)fileId
taskId:(NSNumber *)taskId;

-(NSDictionary *)
setDownload:(NSString *)fileId
taskId:(NSNumber *)taskId;

-(NSDictionary *)readFile:(NSString *)fileId;

-(NSDictionary *)readFilesForPartition:(NSString *)partition;

-(NSDictionary *)readFileWithCollectionPath:(NSString *)fileId;

-(NSDictionary *)downloadComplete:(NSString *)fileId;

-(NSDictionary *)switchToAutoUploadOn:(NSString *)collectionId;

-(NSDictionary *)switchToAutoUploadOff:(NSString *)collectionId;

-(NSDictionary *)removeAllJobs:(NSString *)collectionId;

-(NSDictionary *)readAutoUpload:(NSString *)collectionId;

-(NSDictionary *)convertToDownload:(NSString *)fileId;

-(NSDictionary *)mergeServerDoc:(NSDictionary *)info;

-(NSDictionary *)setNotOnServer:(NSString *)fileId;

-(NSDictionary *)unmergeServerDoc:(NSString *)fileId;

-(NSDictionary *)beginMark:(NSString *)collectionId;

-(NSDictionary *)deleteUnmarked:(NSString *)collectionId;

-(NSDictionary *)markFileAsDeleted:(NSString *)fileId;

-(NSDictionary *)removeDeletedFile:(NSString *)fileId;

@end
