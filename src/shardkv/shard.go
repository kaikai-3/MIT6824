package shardkv

// Shard表示一个分片，包含键值对存储和状态信息
type Shard struct {
	KV     map[string]string // 键值对存储
	Status ShardStatus       // 分片当前状态
}

// NewShard创建并初始化一个新的分片实例
// 初始状态为Serving（正在服务）
func NewShard() *Shard {
	return &Shard{
		KV:     make(map[string]string),
		Status: Serving,
	}
}

// Get获取指定键的值
// 如果键不存在，返回ErrNoKey错误
func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

// deepCopy creates a copy of the shard's key-value pairs.
// Returns a new map with all key-value pairs from the shard.
func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}
