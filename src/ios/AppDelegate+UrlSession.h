#import "AppDelegate.h"

@interface AppDelegate (UrlSession)

- (void)application:(UIApplication *)application
  handleEventsForBackgroundURLSession:(NSString *)identifier
  completionHandler:(void (^)(void))completionHandler;

- (void)application:(UIApplication *)application
  didReceiveLocalNotification:(UILocalNotification *)notification;

@end
