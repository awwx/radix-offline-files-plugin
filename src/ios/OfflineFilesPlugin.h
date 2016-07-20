#import <Foundation/Foundation.h>
#import <Cordova/CDVPlugin.h>

@interface OfflineFilesPlugin : CDVPlugin <NSURLSessionDelegate, NSURLSessionTaskDelegate, NSURLSessionDownloadDelegate>

@end
