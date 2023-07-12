package limit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// Unknown means not initialized state.
	Unknown = iota
	// Allowed means allowed state.
	Allowed
	// HitQuota means this request exactly hit the quota.
	HitQuota
	// OverQuota means passed the quota.
	OverQuota

	internalOverQuota = 0
	internalAllowed   = 1
	internalHitQuota  = 2
)

var (
	// ErrUnknownCode is an error that represents unknown status code.
	ErrUnknownCode = errors.New("unknown status code")

	// to be compatible with aliyun redis, we cannot use `local key = KEYS[1]` to reuse the key
	// --KYES[1]:限流器key 例如openApi场景key可以是商户唯一标识
	// --ARGV[1]:qos,单位时间内最多请求次数
	// --ARGV[2]:单位限流窗口时间
	periodScript = redis.NewScript(`local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCRBY", KEYS[1], 1)
if current == 1 then
    redis.call("expire", KEYS[1], window)
end
if current < limit then
    return 1
elseif current == limit then
    return 2
else
    return 0
end`)
)

type (
	// PeriodOption defines the method to customize a PeriodLimit.
	PeriodOption func(l *PeriodLimit)

	// A PeriodLimit is used to limit requests during a period of time.
	PeriodLimit struct {
		// 窗口大小，单位s
		period int
		// 请求上限
		quota      int
		limitStore *redis.Redis
		keyPrefix  string
		align      bool
	}
)

// NewPeriodLimit returns a PeriodLimit with given parameters.
func NewPeriodLimit(period, quota int, limitStore *redis.Redis, keyPrefix string,
	opts ...PeriodOption) *PeriodLimit {
	limiter := &PeriodLimit{
		period:     period,
		quota:      quota,
		limitStore: limitStore,
		keyPrefix:  keyPrefix,
	}

	for _, opt := range opts {
		opt(limiter)
	}

	return limiter
}

// Take requests a permit, it returns the permit state.
// https://juejin.cn/post/7026645846280110117
func (h *PeriodLimit) Take(key string) (int, error) {
	return h.TakeCtx(context.Background(), key)
}

// TakeCtx requests a permit with context, it returns the permit state.
// 执行限流
// 注意一下返回值：
// 0：表示错误，比如可能是redis故障、过载
// 1：允许
// 2：允许但是当前窗口内已到达上限
// 3：拒绝
func (h *PeriodLimit) TakeCtx(ctx context.Context, key string) (int, error) {
	resp, err := h.limitStore.ScriptRunCtx(ctx, periodScript, []string{h.keyPrefix + key}, []string{
		strconv.Itoa(h.quota),
		strconv.Itoa(h.calcExpireSeconds()),
	})
	if err != nil {
		return Unknown, err
	}

	code, ok := resp.(int64)
	if !ok {
		return Unknown, ErrUnknownCode
	}

	switch code {
	case internalOverQuota:
		return OverQuota, nil
	case internalAllowed:
		return Allowed, nil
	case internalHitQuota:
		return HitQuota, nil
	default:
		return Unknown, ErrUnknownCode
	}
}

// 计算过期时间也就是窗口时间大小
// 如果align==true
// 线性限流，开启此选项后可以实现周期性的限流
// 比如quota=5时，quota实际值可能会是5.4.3.2.1呈现出周期性变化
func (h *PeriodLimit) calcExpireSeconds() int {
	if h.align {
		now := time.Now()
		_, offset := now.Zone()
		unix := now.Unix() + int64(offset)
		return h.period - int(unix%int64(h.period))
	}

	return h.period
}

// Align returns a func to customize a PeriodLimit with alignment.
// For example, if we want to limit end users with 5 sms verification messages every day,
// we need to align with the local timezone and the start of the day.
func Align() PeriodOption {
	return func(l *PeriodLimit) {
		l.align = true
	}
}
