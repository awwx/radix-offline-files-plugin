#import <Foundation/Foundation.h>

typedef void (^Callback)(NSDictionary *);

typedef void (^Operation)(Callback);

@interface Queue : NSObject

-(void)
name:(NSString *)name
run:(Operation)operation
callback:(Callback)callback;

@end
