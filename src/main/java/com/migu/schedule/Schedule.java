package com.migu.schedule;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

/*
 *类名和方法不能修改
 */
public class Schedule {

    // 服务节点nodeid和资源
    private ConcurrentHashMap<String,Server> servers;
    // 挂起任务队列taskid和Task
    private ConcurrentHashMap<String,Task> taskMap;
    // 保存任务id和nodeid
    private ConcurrentHashMap<String, String> schMap;

    /**
     * 内部服务类
     * @author wl
     *
     */
    private class Server{
        private int consumption;
        private int nodeId;
        private List<Task> taskList;

        public int getNodeId() {
            return nodeId;
        }

        public int getConsumption(){
            return consumption;
        }

        public List<Task> getTaskList(){
            return taskList;
        }

        public void clear(){
            consumption=0;
            taskList=new ArrayList<Task>();
        }

        Server(int nodeId){
            this.nodeId=nodeId;
            taskList=new ArrayList<Task>();
        }

        public void add(Task task){
            consumption=consumption+task.getConsumption();
            taskList.add(task);
            schMap.put(String.valueOf(nodeId), String.valueOf(task.getTaskId()));
        }

        public boolean remove(String taskId){
            for(Task task:taskList){
                if(String.valueOf(task.getTaskId()).equals(taskId)){
                    taskList.remove(task);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * 内部任务类
     * @author wl
     *
     */
    private class Task{
        private int taskId;
        private int consumption;
        Task(int taskId,int consumption){
            this.taskId=taskId;
            this.consumption=consumption;
        }

        public int getConsumption(){
            return consumption;
        }

        public int getTaskId(){  return taskId; }
    }


    /**
     * 初始化
     * @return
     */
    public int init() {
        servers=new ConcurrentHashMap<String,Server>();
        taskMap=new ConcurrentHashMap<String,Task>();
        schMap=new ConcurrentHashMap<String,String>();
        return ReturnCodeKeys.E001;
    }


    /**
     * 注册节点
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId) {
        if(nodeId>0){
            if(servers.containsKey(String.valueOf(nodeId))){
                return ReturnCodeKeys.E005;
            }
            Server server=new Server(nodeId);
            servers.put(String.valueOf(nodeId), server);
            return ReturnCodeKeys.E003;
        }
        return ReturnCodeKeys.E004;
    }

    /**
     * 解除任务
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId) {
        if(servers.containsKey(String.valueOf(nodeId))){
            servers.remove(servers.get(String.valueOf(nodeId)));
            return ReturnCodeKeys.E006;
        }
        return ReturnCodeKeys.E007;

    }


    /**
     * 增加任务
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption) {
        if(taskId>0){
            if(taskMap.containsKey(String.valueOf(taskId))){
                return ReturnCodeKeys.E010;
            }else{
                Task task=new Task(taskId,consumption);
                // 挂起队列
                taskMap.put(String.valueOf(taskId), task);
                return ReturnCodeKeys.E008;
            }
        }
        return ReturnCodeKeys.E009;
    }


    /**
     * 删除任务
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId) {
        if(taskMap.containsKey(String.valueOf(taskId))){
            taskMap.remove(String.valueOf(taskId));
            return ReturnCodeKeys.E011;
        }
        Collection<Server> serverList=servers.values();
        boolean result=false;
        for(Server server:serverList){
            result=server.remove(String.valueOf(taskId));
            if(true==result){
                return ReturnCodeKeys.E011;
            }
        }
        return ReturnCodeKeys.E012;
    }


    /**
     * 开始调度
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold) {
        if(threshold>0){
            if(check(threshold)||!taskMap.isEmpty()){
                // 开始调度
                List<Task> dList=new ArrayList<Task>();
                dList.addAll(taskMap.values());
                for(Server server:servers.values()){
                    dList.addAll(server.getTaskList());
                    server.clear();
                }
                // 排序
                Collections.sort(dList, new Comparator<Task>() {
                    public int compare(Task o1, Task o2) {
                        int i = o2.consumption-o1.consumption;
                        return i;
                    }
                });
                int sum=0;

                for(Task task:dList){
                    sum=sum+task.getConsumption();
                }

                divideDebit(dList,servers.values().size(),sum/servers.values().size(),true);
                taskMap=new ConcurrentHashMap<String,Task>();
                if(check(threshold)){
                    return ReturnCodeKeys.E014;
                }
                return ReturnCodeKeys.E013;
            }
            return ReturnCodeKeys.E014;
        }
        return ReturnCodeKeys.E002;
    }


    /**
     * 获取服务数组
     * @return
     */
    private Server[] getServerArray(){
        Server serverArr[]=new Server[servers.size()];
        String[] keys = servers.keySet().toArray(new String[]{});
        for(int i=0;i<keys.length;i++){
            serverArr[i] = servers.get(keys[i]);
        }
        return serverArr;
    }


    /**
     * 校验是否满足调度规则
     * @param threshold
     * @return
     */
    private boolean check(int threshold){
        Server[] sers=getServerArray();
        sort(sers,0);
        int value=sers[sers.length-1].getConsumption()-sers[0].getConsumption();
        if(value>threshold){
            return true;
        }
        return false;
    }


    /**
     * 平均算法
     * @param dList
     * @param num
     * @param direction
     */
    private void divideDebit(List<Task> dList, int num,int avg,
                             boolean isFirst) {
        Server[] sers=getServerArray();
        if(dList.isEmpty()){
            return;
        }
        if(true==isFirst){
            boolean avgFlag=true;
            int avgNum=(num*avg)/dList.size();
            for(Task task:dList){
                if(task.consumption!=avgNum){
                    avgFlag=false;
                }
            }
            if(avgFlag==true){
                int size=dList.size()/num;
                // 直接一次给值吧
                for(int i=0;i<num;i++){
                    for(int j=i*size;j<(i+1)*size;j++)
                        sers[i].add(dList.get(j));
                }
            }else{
                //不排序直接给了
                for(int i=0;i<num;i++){
                    sers[i].add(dList.get(i));
                }
                List<Task> newTask=new ArrayList<Task>();
                for(int i=num;i<dList.size();i++){
                    newTask.add(dList.get(i));
                }
                divideDebit(newTask,num,avg,false);
            }
        }else{
            //排序下,先给小的
            Task task=dList.get(0);
            sort(sers,avg);
            sers[0].add(task);
            dList.remove(0);
            divideDebit(dList,num,avg,false);
        }



    }

    /**
     * 从大到小排序
     * @param a
     */
    private void sort(Server[] a,int avg){
        int len=a.length;//单独把数组长度拿出来，提高效率
        while(len!=0){
            len=len/2;
            for(int i=0;i<len;i++){//分组
                for(int j=i+len;j<a.length;j+=len){//元素从第二个开始
                    int k=j-len;//k为有序序列最后一位的位数
                    Server temp=a[j];//要插入的元素
                    while(k>=0&&(temp.getConsumption()-avg<a[k].getConsumption()-avg)){//从后往前遍历
                        a[k+len]=a[k];
                        k-=len;//向后移动len位
                    }
                    a[k+len]=temp;
                }
            }
        }
    }



    /**
     * 查询状态
     * @param tasks
     * @return
     */
    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(null==tasks){
            return ReturnCodeKeys.E016;
        }
        for(Task task:taskMap.values()){
            TaskInfo taskInfo=new TaskInfo();
            taskInfo.setNodeId(-1);
            taskInfo.setTaskId(task.getTaskId());
            tasks.add(taskInfo);
        }
        for(Server server:servers.values()){
            for(Task task:server.getTaskList()){
                TaskInfo taskInfo=new TaskInfo();
                taskInfo.setNodeId(server.getNodeId());
                taskInfo.setTaskId(task.getTaskId());
                tasks.add(taskInfo);
            }
        }

        Collections.sort(tasks, new Comparator<TaskInfo>() {

            public int compare(TaskInfo o1, TaskInfo o2) {
                int i = o1.getTaskId() - o2.getTaskId();
                return i;
            }
        });
        return ReturnCodeKeys.E015;
    }

}
