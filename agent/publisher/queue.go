package publisher

type Redis interface {
	RPush(string, interface{}) error
	LPop(string, interface{}) error
	IsDocNotExists(error) bool
}

// queueImpl
type queueImpl struct {
	redis Redis
}

func (impl *queueImpl) push(name string, v *message) error {
	return impl.redis.RPush(name, v)
}

func (impl *queueImpl) pop(name string) (v message, err error) {
	err = impl.redis.LPop(name, &v)

	return
}

func (impl *queueImpl) isEmpty(err error) bool {
	return impl.redis.IsDocNotExists(err)
}
