package cn.elton.service;

public interface CacheService {
    //存放
    void  setCommonCache(String key , Object value);

    Object getFromCommonCache(String key);
}
