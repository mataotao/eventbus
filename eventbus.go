package eventbus

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

//BusSubscriber 订阅
type BusSubscriber interface {
	Subscribe(observer interface{})
	SubscribeAsync(observer interface{}, transactional bool)
	SubscribeOnce(observer interface{})
	SubscribeOnceAsync(observer interface{})
	Unsubscribe(observer interface{})
}

//BusPublisher 发布
type BusPublisher interface {
	Publish(topic string, args ...interface{})
}

//BusController 检查
type BusController interface {
	SetMaxTaskNum(num int64)
	Preview() map[string]map[string]struct{}
	HasCallback(topic string) bool
	WaitAsync()
	Quit()
}

//Bus 总线
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
}

// EventBus 事件总线
type EventBus struct {
	handlers *sync.Map
	wg       *sync.WaitGroup
	lock     *sync.Mutex
	maxTask  chan struct{}
}

type eventHandler struct {
	observerType  reflect.Type
	observer      interface{}
	callBack      reflect.Value
	quitCallBack  reflect.Value
	flagOnce      bool
	async         bool
	transactional bool
	lock          *sync.Mutex
}

// New new
func New() Bus {
	b := &EventBus{
		new(sync.Map),
		new(sync.WaitGroup),
		new(sync.Mutex),
		make(chan struct{}, 200),
	}
	return Bus(b)
}

// doSubscribe 处理订阅逻辑
func (bus *EventBus) doSubscribe(topic string, handler *eventHandler) error {
	handlerInterface, _ := bus.handlers.LoadOrStore(topic, make([]*eventHandler, 0))
	handlers := handlerInterface.([]*eventHandler)
	for i := range handlers[:] {
		if handlers[i].observerType.Elem() == handler.observerType.Elem() {
			return nil
		}
	}
	bus.handlers.Store(topic, append(handlers, handler))
	return nil
}

func (bus *EventBus) checkObserver(observer interface{}) ([]string, reflect.Type, string, string) {
	var t reflect.Type
	var fn string
	var quit string
	var ok bool

	t = reflect.TypeOf(observer)
	if t.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("%s is not of type reflect.Struct", t.Kind()))
	}
	fnField, _ := t.Elem().FieldByName("event")
	if fnField.Tag == "" {
		panic(fmt.Sprintf("%v has no field or no fn field", fnField))
	}
	fn, ok = fnField.Tag.Lookup("subscribe")
	if !ok || fn == "" {
		panic("subscribe tag doesn't exist or empty")
	}
	quit, ok = fnField.Tag.Lookup("quit")
	if !ok || quit == "" {
		panic("quit tag doesn't exist or empty")
	}
	topics, ok := fnField.Tag.Lookup("topic")
	if !ok || topics == "" {
		panic("topic tag doesn't exist or empty")
	}
	topic := strings.Split(topics, ",")
	return topic, t, fn, quit
}

func (bus *EventBus) register(observer interface{}, flagOnce, async, transactional bool) {
	topic, t, fn, quit := bus.checkObserver(observer)
	for i := range topic[:] {
		function, ok := t.MethodByName(fn)
		if !ok {
			continue
		}
		quit, ok := t.MethodByName(quit)
		if ok {
			_ = bus.doSubscribe(topic[i], &eventHandler{
				t, observer, function.Func, quit.Func, flagOnce, async, transactional, new(sync.Mutex),
			})
		}
	}
}

// Subscribe 订阅-同步
func (bus *EventBus) Subscribe(observer interface{}) {
	bus.register(observer, false, false, false)
}

// SubscribeAsync  订阅-异步
func (bus *EventBus) SubscribeAsync(observer interface{}, transactional bool) {
	bus.register(observer, false, true, transactional)

}

// SubscribeOnce 订阅-只执行一次-同步
func (bus *EventBus) SubscribeOnce(observer interface{}) {
	bus.register(observer, true, false, false)
}

// SubscribeOnceAsync 订阅-只执行一次-异步
func (bus *EventBus) SubscribeOnceAsync(observer interface{}) {
	bus.register(observer, true, true, false)
}

// HasCallback 查看事件订阅的函数
func (bus *EventBus) HasCallback(topic string) bool {
	handlersInterface, ok := bus.handlers.Load(topic)
	if ok {
		handlers := handlersInterface.([]*eventHandler)
		return len(handlers) > 0
	}
	return false
}

// Unsubscribe 删除订阅
func (bus *EventBus) Unsubscribe(observer interface{}) {
	topic, t, fn, _ := bus.checkObserver(observer)
	for i := range topic[:] {
		function, ok := t.MethodByName(fn)
		if !ok {
			continue
		}
		bus.removeHandler(topic[i], bus.findHandlerIdx(topic[i], function.Func))
	}

}

func (bus *EventBus) removeHandler(topic string, idx int) {
	handlerInterface, ok := bus.handlers.Load(topic)
	if !ok {
		return
	}
	handlers := handlerInterface.([]*eventHandler)
	l := len(handlers)

	if !(0 <= idx && idx < l) {
		return
	}
	handlers = append(handlers[:idx], handlers[idx+1:]...)
	if len(handlers) > 0 {
		bus.handlers.Store(topic, handlers)
	} else {
		bus.handlers.Delete(topic)
	}
}

// Publish 推送
func (bus *EventBus) Publish(topic string, args ...interface{}) {
	if handlerInterface, ok := bus.handlers.Load(topic); ok {
		handlers := handlerInterface.([]*eventHandler)
		if len(handlers) == 0 {
			return
		}
		for i, handler := range handlers {
			if handler.flagOnce {
				bus.removeHandler(topic, i)
			}
			if !handler.async {
				bus.doPublish(handler, args...)
			} else {
				bus.wg.Add(1)
				if handler.transactional {
					handler.lock.Lock()
				}
				bus.maxTask <- struct{}{}
				go bus.doPublishAsync(handlers[i], args...)
			}
		}
	}
}

func (bus *EventBus) doPublish(handler *eventHandler, args ...interface{}) {
	defer func() {
		if err := recover(); err != nil {
			// log.Errorf("eventbus, catch err:%s", err)
		}
	}()
	passedArguments := bus.setUpPublish(handler, args...)
	handler.callBack.Call(passedArguments)
}

func (bus *EventBus) doPublishAsync(handler *eventHandler, args ...interface{}) {
	defer bus.wg.Done()
	defer func() {
		<-bus.maxTask
		if err := recover(); err != nil {
			// log.Errorf("eventbus, catch err:%s", err)
		}
	}()
	if handler.transactional {
		defer handler.lock.Unlock()
	}
	bus.doPublish(handler, args...)

}

func (bus *EventBus) findHandlerIdx(topic string, callback reflect.Value) int {
	if handlerInterface, ok := bus.handlers.Load(topic); ok {
		handlers := handlerInterface.([]*eventHandler)
		for i := range handlers[:] {
			if handlers[i].callBack.Type() == callback.Type() &&
				handlers[i].callBack.Pointer() == callback.Pointer() {
				return i
			}
		}
	}
	return -1
}

func (bus *EventBus) setUpPublish(callback *eventHandler, args ...interface{}) []reflect.Value {
	funcType := callback.callBack.Type()
	passedArguments := make([]reflect.Value, 0, len(args)+1)
	passedArguments = append(passedArguments, reflect.ValueOf(callback.observer))
	for i := range args[:] {
		if args[i] == nil {
			passedArguments = append(passedArguments, reflect.New(funcType.In(i)).Elem())
		} else {
			passedArguments = append(passedArguments, reflect.ValueOf(args[i]))
		}
	}

	return passedArguments
}

// SetMaxTaskNum 设置最大执行任务数
func (bus *EventBus) SetMaxTaskNum(num int64) {
	bus.maxTask = make(chan struct{}, num)
}

// WaitAsync 等待
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}

// Preview 预览任务
func (bus *EventBus) Preview() map[string]map[string]struct{} {
	var s strings.Builder
	res := make(map[string]map[string]struct{})
	bus.handlers.Range(func(topic, value interface{}) bool {
		handlers := value.([]*eventHandler)
		topics := topic.(string)
		s.WriteString(fmt.Sprintf("\n-------------------------\n%s:\n", topic))
		res[topics] = make(map[string]struct{})
		for i := range handlers[:] {
			res[topics][handlers[i].callBack.String()] = struct{}{}
			s.WriteString(fmt.Sprintf("%s\n", handlers[i].callBack.String()))
		}
		return true
	})
	fmt.Println(s.String())
	return res
}

// Quit 退出
func (bus *EventBus) Quit() {
	bus.handlers.Range(func(topic, value interface{}) bool {
		handlers := value.([]*eventHandler)
		for i := range handlers[:] {
			handlers[i].quitCallBack.Call([]reflect.Value{reflect.ValueOf(handlers[i].observer)})
			// _ = bus.Unsubscribe(handlers[i].observer)
		}

		return true
	})
}
