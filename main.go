package main

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(date []byte) uint32

type Map struct {
	hash     Hash                      //使用的 Hash 函数
	replicas int                          //副本数,用于解决少两节点在环上分布不均匀的问题
	keys     []int                        //节点 hash 值排序
	hashMap  map[int]string       //节点 hash 值与节点信息的映射
}

/**
  * Map 初始化接口
  * @param replicas 副本数量, fn 指定的 hash 函数
  * @return 返回创建成功后的节点对象指针
*/
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}

        // 没有指定 hash 函数时指定默认的函数
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}

	return m
}

/**
  * Map 添加节点接口
  * @param keys 要添加的节点信息
*/
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}

        // 对节点 hash 值进行排序,相当于顺时针的环
	sort.Ints(m.keys)
}
/**
  * 在 Map 中删除指定节点
  * @param keys 要删除的节点
*/
func (m *Map) Del(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
		delete(m.hashMap, hash)
	}
}

/**
  * 获取指定key 对应的节点
  * @param key 对应数据的 key
  * @return 返回指定数据对应的节点
*/
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

        // 求余数的原因是当hash 值大于 keys 中的所有时
        // 这个 hash 应该存在环上的第一个节点上
	return m.hashMap[m.keys[idx%len(m.keys)]]
}

