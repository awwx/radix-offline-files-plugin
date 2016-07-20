#import "AppDelegate+UrlSession.h"
#import <objc/runtime.h>

@implementation AppDelegate (UrlSession)

- (id) getCommandInstance:(NSString*)className
{
    return [self.viewController getCommandInstance:className];
}
- (void)application:(UIApplication *)application
handleEventsForBackgroundURLSession:(NSString *)identifier
completionHandler:(void (^)(void))completionHandler
{
  [[self getCommandInstance:@"OfflineFilesPlugin"]
    application:application
    handleEventsForBackgroundURLSession:identifier
    completionHandler:completionHandler];
}

- (void)application:(UIApplication *)application
didReceiveLocalNotification:(UILocalNotification *)notification
{
  [[self getCommandInstance:@"OfflineFilesPlugin"]
    application:application
    didReceiveLocalNotification:notification];
}

@end
