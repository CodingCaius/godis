项目概述：
 Godis 是一个用 Go 语言实现的高性能 Redis 服务器和分布式集群系统。该项目支持多种数据结构，包括string、list、hash、set和sorted set，并提供自动过期（TTL）功能。此外，Godis 还支持 AOF 持久化、发布订阅、事务处理及主从复制等高级功能。  
 
项目技术点：
● 通过 Time Wheel 算法实现自动过期功能，并为系统提供定时任务调度功能。  
● 用跳表（Skip List）实现有序集合的相关功能，确保高效的插入、删除和查找操作。  
● 通过分段锁策略，实现了一个高效的并发安全哈希表，提升了多线程环境下的性能和安全性。  
● 采用哈希槽机制实现数据分片，将单点服务器扩展为分布式缓存系统，提升了系统的可扩展性。    
● 实现了基于 Snowflake 算法的分布式唯一 ID 生成器，确保在分布式环境下生成全局唯一的 ID。  
● 实现了主从复制机制，提高了集群的高可用性。  
● 对于 MSET 等跨节点的命令，使用 TCC（Try-Confirm-Cancel）模式来解决分布式事务问题。  
