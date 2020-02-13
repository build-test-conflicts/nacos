package com.alibaba.nacos.client.config.impl;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient.HttpResult;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.config.utils.MD5;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static com.alibaba.nacos.api.common.Constants.LINE_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.WORD_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.CONFIG_TYPE;
/** 
 * Longpolling
 * @author Nacos
 */
public class ClientWorker {
  private static Logger LOGGER=LogUtils.logger(ClientWorker.class);
  public void addListeners(  String dataId,  String group,  List<? extends Listener> listeners){
    group=null2defaultGroup(group);
    CacheData cache=addCacheDataIfAbsent(dataId,group);
    for (    Listener listener : listeners) {
      cache.addListener(listener);
    }
  }
  public void removeListener(  String dataId,  String group,  Listener listener){
    group=null2defaultGroup(group);
    CacheData cache=getCache(dataId,group);
    if (null != cache) {
      cache.removeListener(listener);
      if (cache.getListeners().isEmpty()) {
        removeCache(dataId,group);
      }
    }
  }
  public void addTenantListeners(  String dataId,  String group,  List<? extends Listener> listeners) throws NacosException {
    group=null2defaultGroup(group);
    String tenant=agent.getTenant();
    CacheData cache=addCacheDataIfAbsent(dataId,group,tenant);
    for (    Listener listener : listeners) {
      cache.addListener(listener);
    }
  }
  public void addTenantListenersWithContent(  String dataId,  String group,  String content,  List<? extends Listener> listeners) throws NacosException {
    group=null2defaultGroup(group);
    String tenant=agent.getTenant();
    CacheData cache=addCacheDataIfAbsent(dataId,group,tenant);
    cache.setContent(content);
    for (    Listener listener : listeners) {
      cache.addListener(listener);
    }
  }
  public void removeTenantListener(  String dataId,  String group,  Listener listener){
    group=null2defaultGroup(group);
    String tenant=agent.getTenant();
    CacheData cache=getCache(dataId,group,tenant);
    if (null != cache) {
      cache.removeListener(listener);
      if (cache.getListeners().isEmpty()) {
        removeCache(dataId,group,tenant);
      }
    }
  }
  void removeCache(  String dataId,  String group){
    String groupKey=GroupKey.getKey(dataId,group);
synchronized (cacheMap) {
      Map<String,CacheData> copy=new HashMap<String,CacheData>(cacheMap.get());
      copy.remove(groupKey);
      cacheMap.set(copy);
    }
    LOGGER.info("[{}] [unsubscribe] {}",agent.getName(),groupKey);
    MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
  }
  void removeCache(  String dataId,  String group,  String tenant){
    String groupKey=GroupKey.getKeyTenant(dataId,group,tenant);
synchronized (cacheMap) {
      Map<String,CacheData> copy=new HashMap<String,CacheData>(cacheMap.get());
      copy.remove(groupKey);
      cacheMap.set(copy);
    }
    LOGGER.info("[{}] [unsubscribe] {}",agent.getName(),groupKey);
    MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
  }
  public CacheData addCacheDataIfAbsent(  String dataId,  String group){
    CacheData cache=getCache(dataId,group);
    if (null != cache) {
      return cache;
    }
    String key=GroupKey.getKey(dataId,group);
    cache=new CacheData(configFilterChainManager,agent.getName(),dataId,group);
synchronized (cacheMap) {
      CacheData cacheFromMap=getCache(dataId,group);
      if (null != cacheFromMap) {
        cache=cacheFromMap;
        cache.setInitializing(true);
      }
 else {
        int taskId=cacheMap.get().size() / (int)ParamUtil.getPerTaskConfigSize();
        cache.setTaskId(taskId);
      }
      Map<String,CacheData> copy=new HashMap<String,CacheData>(cacheMap.get());
      copy.put(key,cache);
      cacheMap.set(copy);
    }
    LOGGER.info("[{}] [subscribe] {}",agent.getName(),key);
    MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
    return cache;
  }
  public CacheData addCacheDataIfAbsent(  String dataId,  String group,  String tenant) throws NacosException {
    CacheData cache=getCache(dataId,group,tenant);
    if (null != cache) {
      return cache;
    }
    String key=GroupKey.getKeyTenant(dataId,group,tenant);
synchronized (cacheMap) {
      CacheData cacheFromMap=getCache(dataId,group,tenant);
      if (null != cacheFromMap) {
        cache=cacheFromMap;
        cache.setInitializing(true);
      }
 else {
        cache=new CacheData(configFilterChainManager,agent.getName(),dataId,group,tenant);
        if (enableRemoteSyncConfig) {
          String[] ct=getServerConfig(dataId,group,tenant,3000L);
          cache.setContent(ct[0]);
        }
      }
      Map<String,CacheData> copy=new HashMap<String,CacheData>(cacheMap.get());
      copy.put(key,cache);
      cacheMap.set(copy);
    }
    LOGGER.info("[{}] [subscribe] {}",agent.getName(),key);
    MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
    return cache;
  }
  public CacheData getCache(  String dataId,  String group){
    return getCache(dataId,group,TenantUtil.getUserTenantForAcm());
  }
  public CacheData getCache(  String dataId,  String group,  String tenant){
    if (null == dataId || null == group) {
      throw new IllegalArgumentException();
    }
    return cacheMap.get().get(GroupKey.getKeyTenant(dataId,group,tenant));
  }
  public String[] getServerConfig(  String dataId,  String group,  String tenant,  long readTimeout) throws NacosException {
    String[] ct=new String[2];
    if (StringUtils.isBlank(group)) {
      group=Constants.DEFAULT_GROUP;
    }
    HttpResult result=null;
    try {
      List<String> params=null;
      if (StringUtils.isBlank(tenant)) {
        params=new ArrayList<String>(Arrays.asList("dataId",dataId,"group",group));
      }
 else {
        params=new ArrayList<String>(Arrays.asList("dataId",dataId,"group",group,"tenant",tenant));
      }
      result=agent.httpGet(Constants.CONFIG_CONTROLLER_PATH,null,params,agent.getEncode(),readTimeout);
    }
 catch (    IOException e) {
      String message=String.format("[%s] [sub-server] get server config exception, dataId=%s, group=%s, tenant=%s",agent.getName(),dataId,group,tenant);
      LOGGER.error(message,e);
      throw new NacosException(NacosException.SERVER_ERROR,e);
    }
switch (result.code) {
case HttpURLConnection.HTTP_OK:
      LocalConfigInfoProcessor.saveSnapshot(agent.getName(),dataId,group,tenant,result.content);
    ct[0]=result.content;
  if (result.headers.containsKey(CONFIG_TYPE)) {
    ct[1]=result.headers.get(CONFIG_TYPE).get(0);
  }
 else {
    ct[1]=ConfigType.TEXT.getType();
  }
return ct;
case HttpURLConnection.HTTP_NOT_FOUND:
LocalConfigInfoProcessor.saveSnapshot(agent.getName(),dataId,group,tenant,null);
return ct;
case HttpURLConnection.HTTP_CONFLICT:
{
LOGGER.error("[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, " + "tenant={}",agent.getName(),dataId,group,tenant);
throw new NacosException(NacosException.CONFLICT,"data being modified, dataId=" + dataId + ",group="+ group+ ",tenant="+ tenant);
}
case HttpURLConnection.HTTP_FORBIDDEN:
{
LOGGER.error("[{}] [sub-server-error] no right, dataId={}, group={}, tenant={}",agent.getName(),dataId,group,tenant);
throw new NacosException(result.code,result.content);
}
default :
{
LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}",agent.getName(),dataId,group,tenant,result.code);
throw new NacosException(result.code,"http error, code=" + result.code + ",dataId="+ dataId+ ",group="+ group+ ",tenant="+ tenant);
}
}
}
private void checkLocalConfig(CacheData cacheData){
final String dataId=cacheData.dataId;
final String group=cacheData.group;
final String tenant=cacheData.tenant;
File path=LocalConfigInfoProcessor.getFailoverFile(agent.getName(),dataId,group,tenant);
if (!cacheData.isUseLocalConfigInfo() && path.exists()) {
String content=LocalConfigInfoProcessor.getFailover(agent.getName(),dataId,group,tenant);
String md5=MD5.getInstance().getMD5String(content);
cacheData.setUseLocalConfigInfo(true);
cacheData.setLocalConfigInfoVersion(path.lastModified());
cacheData.setContent(content);
LOGGER.warn("[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",agent.getName(),dataId,group,tenant,md5,ContentUtils.truncateContent(content));
return;
}
if (cacheData.isUseLocalConfigInfo() && !path.exists()) {
cacheData.setUseLocalConfigInfo(false);
LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}",agent.getName(),dataId,group,tenant);
return;
}
if (cacheData.isUseLocalConfigInfo() && path.exists() && cacheData.getLocalConfigInfoVersion() != path.lastModified()) {
String content=LocalConfigInfoProcessor.getFailover(agent.getName(),dataId,group,tenant);
String md5=MD5.getInstance().getMD5String(content);
cacheData.setUseLocalConfigInfo(true);
cacheData.setLocalConfigInfoVersion(path.lastModified());
cacheData.setContent(content);
LOGGER.warn("[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",agent.getName(),dataId,group,tenant,md5,ContentUtils.truncateContent(content));
}
}
private String null2defaultGroup(String group){
return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
}
public void checkConfigInfo(){
int listenerSize=cacheMap.get().size();
int longingTaskCount=(int)Math.ceil(listenerSize / ParamUtil.getPerTaskConfigSize());
if (longingTaskCount > currentLongingTaskCount) {
for (int i=(int)currentLongingTaskCount; i < longingTaskCount; i++) {
executorService.execute(new LongPollingRunnable(i));
}
currentLongingTaskCount=longingTaskCount;
}
}
/** 
 * 从Server获取值变化了的DataID列表。返回的对象里只有dataId和group是有效的。 保证不返回NULL。
 */
List<String> checkUpdateDataIds(List<CacheData> cacheDatas,List<String> inInitializingCacheList) throws IOException {
StringBuilder sb=new StringBuilder();
for (CacheData cacheData : cacheDatas) {
if (!cacheData.isUseLocalConfigInfo()) {
sb.append(cacheData.dataId).append(WORD_SEPARATOR);
sb.append(cacheData.group).append(WORD_SEPARATOR);
if (StringUtils.isBlank(cacheData.tenant)) {
sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);
}
 else {
sb.append(cacheData.getMd5()).append(WORD_SEPARATOR);
sb.append(cacheData.getTenant()).append(LINE_SEPARATOR);
}
if (cacheData.isInitializing()) {
inInitializingCacheList.add(GroupKey.getKeyTenant(cacheData.dataId,cacheData.group,cacheData.tenant));
}
}
}
boolean isInitializingCacheList=!inInitializingCacheList.isEmpty();
return checkUpdateConfigStr(sb.toString(),isInitializingCacheList);
}
/** 
 * 从Server获取值变化了的DataID列表。返回的对象里只有dataId和group是有效的。 保证不返回NULL。
 */
List<String> checkUpdateConfigStr(String probeUpdateString,boolean isInitializingCacheList) throws IOException {
List<String> params=Arrays.asList(Constants.PROBE_MODIFY_REQUEST,probeUpdateString);
List<String> headers=new ArrayList<String>(2);
headers.add("Long-Pulling-Timeout");
headers.add("" + timeout);
if (isInitializingCacheList) {
headers.add("Long-Pulling-Timeout-No-Hangup");
headers.add("true");
}
if (StringUtils.isBlank(probeUpdateString)) {
return Collections.emptyList();
}
try {
long readTimeoutMs=timeout + (long)Math.round(timeout >> 1);
HttpResult result=agent.httpPost(Constants.CONFIG_CONTROLLER_PATH + "/listener",headers,params,agent.getEncode(),readTimeoutMs);
if (HttpURLConnection.HTTP_OK == result.code) {
setHealthServer(true);
return parseUpdateDataIdResponse(result.content);
}
 else {
setHealthServer(false);
LOGGER.error("[{}] [check-update] get changed dataId error, code: {}",agent.getName(),result.code);
}
}
 catch (IOException e) {
setHealthServer(false);
LOGGER.error("[" + agent.getName() + "] [check-update] get changed dataId exception",e);
throw e;
}
return Collections.emptyList();
}
/** 
 * 从HTTP响应拿到变化的groupKey。保证不返回NULL。
 */
private List<String> parseUpdateDataIdResponse(String response){
if (StringUtils.isBlank(response)) {
return Collections.emptyList();
}
try {
response=URLDecoder.decode(response,"UTF-8");
}
 catch (Exception e) {
LOGGER.error("[" + agent.getName() + "] [polling-resp] decode modifiedDataIdsString error",e);
}
List<String> updateList=new LinkedList<String>();
for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
if (!StringUtils.isBlank(dataIdAndGroup)) {
String[] keyArr=dataIdAndGroup.split(WORD_SEPARATOR);
String dataId=keyArr[0];
String group=keyArr[1];
if (keyArr.length == 2) {
updateList.add(GroupKey.getKey(dataId,group));
LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}",agent.getName(),dataId,group);
}
 else if (keyArr.length == 3) {
String tenant=keyArr[2];
updateList.add(GroupKey.getKeyTenant(dataId,group,tenant));
LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}, tenant={}",agent.getName(),dataId,group,tenant);
}
 else {
LOGGER.error("[{}] [polling-resp] invalid dataIdAndGroup error {}",agent.getName(),dataIdAndGroup);
}
}
}
return updateList;
}
@SuppressWarnings("PMD.ThreadPoolCreationRule") public ClientWorker(final HttpAgent agent,final ConfigFilterChainManager configFilterChainManager,final Properties properties){
this.agent=agent;
this.configFilterChainManager=configFilterChainManager;
init(properties);
executor=Executors.newScheduledThreadPool(1,new ThreadFactory(){
@Override public Thread newThread(Runnable r){
Thread t=new Thread(r);
t.setName("com.alibaba.nacos.client.Worker." + agent.getName());
t.setDaemon(true);
return t;
}
}
);
executorService=Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),new ThreadFactory(){
@Override public Thread newThread(Runnable r){
Thread t=new Thread(r);
t.setName("com.alibaba.nacos.client.Worker.longPolling." + agent.getName());
t.setDaemon(true);
return t;
}
}
);
executor.scheduleWithFixedDelay(new Runnable(){
@Override public void run(){
try {
checkConfigInfo();
}
 catch (Throwable e) {
LOGGER.error("[" + agent.getName() + "] [sub-check] rotate check error",e);
}
}
}
,1L,10L,TimeUnit.MILLISECONDS);
}
private void init(Properties properties){
timeout=Math.max(NumberUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),Constants.CONFIG_LONG_POLL_TIMEOUT),Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);
taskPenaltyTime=NumberUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME),Constants.CONFIG_RETRY_TIME);
enableRemoteSyncConfig=Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
}
class LongPollingRunnable implements Runnable {
private int taskId;
public LongPollingRunnable(int taskId){
this.taskId=taskId;
}
@Override public void run(){
List<CacheData> cacheDatas=new ArrayList<CacheData>();
List<String> inInitializingCacheList=new ArrayList<String>();
try {
for (CacheData cacheData : cacheMap.get().values()) {
if (cacheData.getTaskId() == taskId) {
cacheDatas.add(cacheData);
try {
checkLocalConfig(cacheData);
if (cacheData.isUseLocalConfigInfo()) {
cacheData.checkListenerMd5();
}
}
 catch (Exception e) {
LOGGER.error("get local config info error",e);
}
}
}
List<String> changedGroupKeys=checkUpdateDataIds(cacheDatas,inInitializingCacheList);
for (String groupKey : changedGroupKeys) {
String[] key=GroupKey.parseKey(groupKey);
String dataId=key[0];
String group=key[1];
String tenant=null;
if (key.length == 3) {
tenant=key[2];
}
try {
String[] ct=getServerConfig(dataId,group,tenant,3000L);
CacheData cache=cacheMap.get().get(GroupKey.getKeyTenant(dataId,group,tenant));
cache.setContent(ct[0]);
if (null != ct[1]) {
cache.setType(ct[1]);
}
LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",agent.getName(),dataId,group,tenant,cache.getMd5(),ContentUtils.truncateContent(ct[0]),ct[1]);
}
 catch (NacosException ioe) {
String message=String.format("[%s] [get-update] get changed config exception. dataId=%s, group=%s, tenant=%s",agent.getName(),dataId,group,tenant);
LOGGER.error(message,ioe);
}
}
for (CacheData cacheData : cacheDatas) {
if (!cacheData.isInitializing() || inInitializingCacheList.contains(GroupKey.getKeyTenant(cacheData.dataId,cacheData.group,cacheData.tenant))) {
cacheData.checkListenerMd5();
cacheData.setInitializing(false);
}
}
inInitializingCacheList.clear();
executorService.execute(this);
}
 catch (Throwable e) {
LOGGER.error("longPolling error : ",e);
executorService.schedule(this,taskPenaltyTime,TimeUnit.MILLISECONDS);
}
}
}
public boolean isHealthServer(){
return isHealthServer;
}
private void setHealthServer(boolean isHealthServer){
this.isHealthServer=isHealthServer;
}
ScheduledExecutorService executor;
ScheduledExecutorService executorService;
/** 
 * groupKey -> cacheData
 */
private AtomicReference<Map<String,CacheData>> cacheMap=new AtomicReference<Map<String,CacheData>>(new HashMap<String,CacheData>());
private HttpAgent agent;
private ConfigFilterChainManager configFilterChainManager;
private boolean isHealthServer=true;
private long timeout;
private double currentLongingTaskCount=0;
private int taskPenaltyTime;
private boolean enableRemoteSyncConfig=false;
public LongPollingRunnable(){
}
}
