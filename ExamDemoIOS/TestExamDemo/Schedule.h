//
//  Scheduler.h
//  ExamDemo
//
//  Created by George She on 2018/6/8.
//  Copyright © 2018年 CMRead. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "TaskInfo.h"

@interface Schedule : NSObject
-(int)clean;
-(int)registerNode:(int)nodeId;
-(int)unregisterNode:(int)nodeId;

-(int)addTask:(int)taskId withConsumption:(int)consumption;
-(int)deleteTask:(int)taskId;

-(int)scheduleTask:(int)threshold;
-(int)queryTaskStatus:(NSMutableArray<TaskInfo *> *)tasks;
@end
