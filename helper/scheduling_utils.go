package helper

type RunnableFun func()

type Runnable struct {
	r RunnableFun
}

func NewRunnable(r RunnableFun) *Runnable {
	return &Runnable{
		r,
	}
}
func (s *Runnable) Run() {
	s.r()
}
