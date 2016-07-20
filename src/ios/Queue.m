#import "Queue.h"
#import "Data.h"

static BOOL debug = NO;

@interface Task : NSObject

@property NSString *name;
@property Operation operation;
@property Callback callback;

@end

@implementation Task

+(Task *)
name:(NSString *)name
operation:(Operation)operation
callback:(Callback)callback
{
  Task *task = [[Task alloc] init];
  task.name = name;
  task.operation = operation;
  task.callback = callback;
  return task;
}

@end


@interface Queue ()

@property NSMutableArray* queue;
@property BOOL running;

@end

@implementation Queue

-(id)init {
  self = [super init];
  if (self) {
    _queue = [NSMutableArray arrayWithCapacity:10];
    _running = NO;
  }
  return self;
}

-(void)
start:(Task *)task
{
  if (self.running)
    [NSException raise:@"oops" format:@"already running"];

  self.running = true;

  dispatch_async(
    dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
    ^{
      @try {
        if (debug)
          NSLog(@"&& --->>> begin %@", task.name);

        task.operation(^(NSDictionary *result){
          if (debug)
            NSLog(@"&& <<<--- end %@", task.name);

          bool empty;
          @synchronized(self) {
            self.running = false;
            empty = self.queue.count == 0;
            if (self.queue.count != 0) {
              Task *next = [self.queue objectAtIndex:0];
              [self.queue removeObjectAtIndex:0];
              [self start:next];
            }
            if (empty)
              if (debug)
                NSLog(@"&& .... queue empty ....");
          }
          dispatch_async(dispatch_get_main_queue(), ^{
            task.callback(result);
          });
        });
      }
      @catch (NSException *exception) {
        NSDictionary *error = [Data exceptionResult:exception];
        dispatch_async(dispatch_get_main_queue(), ^{
          task.callback(error);
        });
      }
    }
  );
}

-(void)
name:(NSString *)name
run:(Operation)operation
callback:(Callback)callback
{
  if (debug)
    NSLog(@"&& --- queue %@", name);

  Task *task = [Task name:name operation:operation callback:callback];

  @synchronized(self) {
    if (self.running) {
      [self.queue addObject:task];
    } else {
      [self start:task];
    }
  }
}

@end
