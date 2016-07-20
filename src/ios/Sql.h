#import <Foundation/Foundation.h>

@interface Sql : NSObject

-(NSString *) open;

-(NSArray *)
executeSql:(NSString *)sql
withSqlArgs:(NSArray *)sqlArgs
withReadOnly:(BOOL)readOnly;

@end
