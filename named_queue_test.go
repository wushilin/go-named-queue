package namedqueue

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// The scenarios here mirror the integration tests of the Rust named_queue
// crate (tests/*.rs in 0.3.0), adapted to the Go API.

const wait = 50 * time.Millisecond

func mustCreate[T any](t *testing.T, r *QueueRegistry, name string, capacity int) {
	t.Helper()
	if err := Create[T](r, name, capacity); err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
}

func mustCreateBroadcast[T any](t *testing.T, r *QueueRegistry, name string, capacity int, action FullAction) {
	t.Helper()
	if err := CreateBroadcastingWithFullAction[T](r, name, capacity, action); err != nil {
		t.Fatalf("CreateBroadcasting(%q): %v", name, err)
	}
}

func mustSender[T any](t *testing.T, r *QueueRegistry, name string) *Sender[T] {
	t.Helper()
	tx, err := AcquireSender[T](r, name)
	if err != nil {
		t.Fatalf("AcquireSender(%q): %v", name, err)
	}
	return tx
}

func mustReceiver[T any](t *testing.T, r *QueueRegistry, name string) *Receiver[T] {
	t.Helper()
	rx, err := AcquireReceiver[T](r, name)
	if err != nil {
		t.Fatalf("AcquireReceiver(%q): %v", name, err)
	}
	return rx
}

func mustRecv[T any](t *testing.T, rx *Receiver[T]) T {
	t.Helper()
	v, err := rx.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	return v
}

func wantErr(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("got error %v, want %v", got, want)
	}
}

func wantState(t *testing.T, r *QueueRegistry, name string, want State) {
	t.Helper()
	got, err := r.State(name)
	if err != nil {
		t.Fatalf("State(%q): %v", name, err)
	}
	if got != want {
		t.Fatalf("State(%q) = %+v, want %+v", name, got, want)
	}
}

// blocks reports whether fn is still running after `wait`. The goroutine
// running fn is left to finish on its own.
func blocks(fn func()) (done chan struct{}, blocked bool) {
	done = make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
		return done, false
	case <-time.After(wait):
		return done, true
	}
}

func finishes(t *testing.T, done chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s never finished", what)
	}
}

// ---- create ----

func TestCreateRegistersQueue(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	wantState(t, r, "q", State{Open: true})
}

func TestCreateRejectsDuplicateNamesEvenAcrossTypesAndKinds(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	wantErr(t, Create[int](r, "q", 4), ErrExists)
	wantErr(t, Create[string](r, "q", 4), ErrExists)
	wantErr(t, CreateBroadcasting[int](r, "q", 4), ErrExists)
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	wantErr(t, Create[int](r, "b", 4), ErrExists)
}

func TestCreateValidatesArguments(t *testing.T) {
	r := NewRegistry()
	wantErr(t, Create[int](r, "q", 0), ErrInvalidCapacity)
	wantErr(t, Create[int](r, "q", -1), ErrInvalidCapacity)
	wantErr(t, CreateBroadcasting[int](r, "q", 0), ErrInvalidCapacity)
	wantErr(t, CreateWithFullAction[int](r, "q", 1, FullAction(99)), ErrInvalidFullAction)
	wantErr(t, CreateBroadcastingWithFullAction[int](r, "q", 1, FullAction(99)), ErrInvalidFullAction)
	if _, err := r.State("q"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected create must not register the name: %v", err)
	}
}

func TestDistinctNamesCoexist(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "a", 1)
	mustCreate[string](t, r, "b", 1)
	mustSender[int](t, r, "a").Send(1)
	mustSender[string](t, r, "b").Send("x")
	if got := mustRecv(t, mustReceiver[int](t, r, "a")); got != 1 {
		t.Fatal(got)
	}
	if got := mustRecv(t, mustReceiver[string](t, r, "b")); got != "x" {
		t.Fatal(got)
	}
}

func TestClonesShareStateButNewRegistriesAreIndependent(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	if _, err := r.Clone().State("q"); err != nil {
		t.Fatalf("clone should see queue: %v", err)
	}
	if _, err := NewRegistry().State("q"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("fresh registry should not see queue: %v", err)
	}
}

func TestZeroValueRegistryIsUsableConcurrently(t *testing.T) {
	var r QueueRegistry
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = Create[int](&r, fmt.Sprint("q", i), 1)
		}(i)
	}
	wg.Wait()
	for i := 0; i < 16; i++ {
		if _, err := r.State(fmt.Sprint("q", i)); err != nil {
			t.Fatalf("q%d missing: all goroutines must share one registry (%v)", i, err)
		}
	}
	if (*QueueRegistry)(nil).Shutdown("q0") != ErrNotFound {
		t.Fatal("nil registry should report ErrNotFound")
	}
}

func TestDefaultRegistryHelpersShareOneRegistry(t *testing.T) {
	name := fmt.Sprintf("default-%d", time.Now().UnixNano())
	if err := CreateDefault[int](name, 2); err != nil {
		t.Fatal(err)
	}
	defer Destroy(name)
	tx, err := AcquireDefaultSender[int](name)
	if err != nil {
		t.Fatal(err)
	}
	tx.Send(5)
	if st, _ := QueueState(name); st.Pending != 1 {
		t.Fatalf("pending = %d", st.Pending)
	}
	rx, err := AcquireReceiver[int](DefaultRegistry(), name)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustRecv(t, rx); got != 5 {
		t.Fatal(got)
	}
	if err := Shutdown(name); err != nil {
		t.Fatal(err)
	}
	wantErr(t, tx.Send(6), ErrClosed)
}

// ---- send / recv ----

func TestRoundtrip(t *testing.T) {
	r := NewRegistry()
	mustCreate[string](t, r, "q", 4)
	tx := mustSender[string](t, r, "q")
	rx := mustReceiver[string](t, r, "q")
	if err := tx.Send("hello"); err != nil {
		t.Fatal(err)
	}
	wantState(t, r, "q", State{Open: true, Pending: 1})
	if got := mustRecv(t, rx); got != "hello" {
		t.Fatal(got)
	}
	wantState(t, r, "q", State{Open: true, Pending: 0})
}

func TestAcquireOnMissingQueueIsNotFound(t *testing.T) {
	r := NewRegistry()
	if _, err := AcquireSender[int](r, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := AcquireReceiver[int](r, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestAcquireWithWrongTypeIsTypeMismatch(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	for _, name := range []string{"q", "b"} {
		_, err := AcquireSender[string](r, name)
		wantErr(t, err, ErrTypeMismatch)
		_, err = AcquireReceiver[string](r, name)
		wantErr(t, err, ErrTypeMismatch)
	}
	_, err := AcquireSender[string](r, "q")
	if want := `queue "q" contains int, requested string`; err == nil || !contains(err.Error(), want) {
		t.Fatalf("error should name both types: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestTrySendFullAndTryRecvEmpty(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	tx := mustSender[int](t, r, "q")
	rx := mustReceiver[int](t, r, "q")
	if _, err := rx.TryRecv(); !errors.Is(err, ErrEmpty) {
		t.Fatal(err)
	}
	if err := tx.TrySend(1); err != nil {
		t.Fatal(err)
	}
	wantErr(t, tx.TrySend(2), ErrFull)
	if v, err := rx.TryRecv(); err != nil || v != 1 {
		t.Fatal(v, err)
	}
}

func TestSendBlocksWhenFullUntilRecv(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	tx := mustSender[int](t, r, "q")
	rx := mustReceiver[int](t, r, "q")
	tx.Send(1)
	done, blocked := blocks(func() { tx.Send(2) })
	if !blocked {
		t.Fatal("send into a full queue should block")
	}
	mustRecv(t, rx)
	finishes(t, done, "blocked send")
	if got := mustRecv(t, rx); got != 2 {
		t.Fatal(got)
	}
}

func TestClonesShareOneQueue(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	tx := mustSender[int](t, r, "q")
	tx2 := tx.Clone()
	rx := mustReceiver[int](t, r, "q")
	rx2 := rx.Clone()
	tx.Send(1)
	tx2.Send(2)
	got := []int{mustRecv(t, rx2), mustRecv(t, rx)}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
	// Close is a no-op on work-sharing receivers; the clone keeps working.
	rx.Close()
	tx.Send(3)
	if got := mustRecv(t, rx); got != 3 {
		t.Fatal(got)
	}
}

func TestMessagesNeedNoSpecialTraits(t *testing.T) {
	type opaque struct {
		fn func() int
		ch chan int
	}
	r := NewRegistry()
	mustCreate[opaque](t, r, "q", 1)
	mustSender[opaque](t, r, "q").Send(opaque{fn: func() int { return 7 }})
	if got := mustRecv(t, mustReceiver[opaque](t, r, "q")); got.fn() != 7 {
		t.Fatal("message lost")
	}
}

func TestMPMCEndToEndUnderBackpressure(t *testing.T) {
	const producers, consumers, perProducer = 4, 3, 500
	r := NewRegistry()
	mustCreate[int](t, r, "q", 2)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		tx := mustSender[int](t, r, "q")
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := tx.Send(p*perProducer + i); err != nil {
					t.Error(err)
					return
				}
			}
		}(p)
	}
	var mu sync.Mutex
	var got []int
	var cwg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		rx := mustReceiver[int](t, r, "q")
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				v, err := rx.Recv()
				if err != nil {
					return
				}
				mu.Lock()
				got = append(got, v)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := r.Shutdown("q"); err != nil {
		t.Fatal(err)
	}
	cwg.Wait()
	sort.Ints(got)
	if len(got) != producers*perProducer {
		t.Fatalf("got %d messages, want %d", len(got), producers*perProducer)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("message %d missing or duplicated (saw %d)", i, v)
		}
	}
}

// ---- shutdown ----

func TestShutdownUnknownNameIsNotFound(t *testing.T) {
	wantErr(t, NewRegistry().Shutdown("nope"), ErrNotFound)
}

func TestShutdownCutsSendersButLetsReceiversDrain(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	tx := mustSender[int](t, r, "q")
	rx := mustReceiver[int](t, r, "q")
	tx.Send(1)
	tx.Send(2)
	if err := r.Shutdown("q"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, tx.Send(3), ErrClosed)
	wantErr(t, tx.TrySend(3), ErrClosed)
	wantState(t, r, "q", State{Open: false, Pending: 2})
	if got := []int{mustRecv(t, rx), mustRecv(t, rx)}; !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
	_, err := rx.Recv()
	wantErr(t, err, ErrClosed)
	_, err = rx.TryRecv()
	wantErr(t, err, ErrClosed)
}

func TestShutdownTwiceIsClosed(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	for _, name := range []string{"q", "b"} {
		if err := r.Shutdown(name); err != nil {
			t.Fatal(err)
		}
		wantErr(t, r.Shutdown(name), ErrClosed)
	}
}

func TestShutdownQueueIssuesNoNewSenders(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	r.Shutdown("q")
	r.Shutdown("b")
	for _, name := range []string{"q", "b"} {
		_, err := AcquireSender[int](r, name)
		wantErr(t, err, ErrClosed)
	}
}

func TestClosedQueueAdmitsNewReceiversWhileMessagesRemain(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	mustSender[int](t, r, "q").Send(1)
	r.Shutdown("q")
	late := mustReceiver[int](t, r, "q") // messages remain: admitted
	if got := mustRecv(t, late); got != 1 {
		t.Fatal(got)
	}
	_, err := AcquireReceiver[int](r, "q") // drained: refused
	wantErr(t, err, ErrClosed)
}

func TestShutdownWithNoReceiversRetainsMessagesForLateDrain(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	tx := mustSender[int](t, r, "q")
	tx.Send(1)
	tx.Send(2)
	r.Shutdown("q")
	wantState(t, r, "q", State{Open: false, Pending: 2})
	rx := mustReceiver[int](t, r, "q")
	if got := []int{mustRecv(t, rx), mustRecv(t, rx)}; !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
}

func TestBlockedReceiverWakesWithClosedOnShutdown(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	rx := mustReceiver[int](t, r, "q")
	var err error
	done, blocked := blocks(func() { _, err = rx.Recv() })
	if !blocked {
		t.Fatal("recv on empty queue should block")
	}
	r.Shutdown("q")
	finishes(t, done, "blocked recv")
	wantErr(t, err, ErrClosed)
}

func TestBlockedSenderWakesWithClosedOnShutdown(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	tx := mustSender[int](t, r, "q")
	tx.Send(1)
	var err error
	done, blocked := blocks(func() { err = tx.Send(2) })
	if !blocked {
		t.Fatal("send on full queue should block")
	}
	r.Shutdown("q")
	finishes(t, done, "blocked send")
	wantErr(t, err, ErrClosed)
	wantState(t, r, "q", State{Open: false, Pending: 1})
}

// ---- state / destroy ----

func TestStateOfUnknownQueueIsNotFound(t *testing.T) {
	if _, err := NewRegistry().State("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestDestroyDiscardsPendingAndFreesName(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	tx := mustSender[int](t, r, "q")
	rx := mustReceiver[int](t, r, "q")
	tx.Send(1)
	if err := r.Destroy("q"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, r.Destroy("q"), ErrNotFound)
	if _, err := r.State("q"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	wantErr(t, tx.Send(2), ErrClosed)
	_, err := rx.Recv()
	wantErr(t, err, ErrClosed)
	_, err = rx.TryRecv()
	wantErr(t, err, ErrClosed)
	mustCreate[string](t, r, "q", 1) // name reusable, even with another type
}

func TestDestroyWakesBlockedReceiversAndSenders(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 1)
	tx := mustSender[int](t, r, "q")
	rx := mustReceiver[int](t, r, "q")
	var recvErr error
	recvDone, _ := blocks(func() { _, recvErr = rx.Recv() })
	tx.Send(1)
	finishes(t, recvDone, "recv")
	var sendErr error
	tx.Send(1)
	sendDone, blocked := blocks(func() { sendErr = tx.Send(2) })
	if !blocked {
		t.Fatal("send should block")
	}
	r.Destroy("q")
	finishes(t, sendDone, "blocked send")
	wantErr(t, sendErr, ErrClosed)
	_ = recvErr
}

func TestDestroyTakeReturnsUnconsumedMessages(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	tx := mustSender[int](t, r, "q")
	tx.Send(1)
	tx.Send(2)
	tx.Send(3)
	got, err := DestroyTake[int](r, "q")
	if err != nil || !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatal(got, err)
	}
	if _, err := r.State("q"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := DestroyTake[int](r, "q"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestDestroyTakeWithWrongTypeIsTypeMismatchAndKeepsQueue(t *testing.T) {
	r := NewRegistry()
	mustCreate[int](t, r, "q", 4)
	mustSender[int](t, r, "q").Send(1)
	_, err := DestroyTake[string](r, "q")
	wantErr(t, err, ErrTypeMismatch)
	wantState(t, r, "q", State{Open: true, Pending: 1})
}

// ---- full-buffer policies (work-sharing) ----

func TestQueueDropOldestNeverBlocks(t *testing.T) {
	r := NewRegistry()
	if err := CreateWithFullAction[int](r, "q", 2, FullDropOldest); err != nil {
		t.Fatal(err)
	}
	tx := mustSender[int](t, r, "q")
	for i := 1; i <= 5; i++ {
		if err := tx.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.TrySend(6); err != nil {
		t.Fatal(err)
	}
	rx := mustReceiver[int](t, r, "q")
	if got := []int{mustRecv(t, rx), mustRecv(t, rx)}; !reflect.DeepEqual(got, []int{5, 6}) {
		t.Fatal(got)
	}
}

func TestQueueDropNewestNeverBlocks(t *testing.T) {
	r := NewRegistry()
	if err := CreateWithFullAction[int](r, "q", 2, FullDropNewest); err != nil {
		t.Fatal(err)
	}
	tx := mustSender[int](t, r, "q")
	for i := 1; i <= 5; i++ {
		if err := tx.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.TrySend(6); err != nil {
		t.Fatal(err)
	}
	rx := mustReceiver[int](t, r, "q")
	if got := []int{mustRecv(t, rx), mustRecv(t, rx)}; !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
	if _, err := rx.TryRecv(); !errors.Is(err, ErrEmpty) {
		t.Fatal(err)
	}
}

func TestQueueDropModeSendAfterShutdownIsClosed(t *testing.T) {
	r := NewRegistry()
	if err := CreateWithFullAction[int](r, "q", 1, FullDropOldest); err != nil {
		t.Fatal(err)
	}
	tx := mustSender[int](t, r, "q")
	tx.Send(1)
	r.Shutdown("q")
	wantErr(t, tx.Send(2), ErrClosed)
	wantErr(t, tx.TrySend(2), ErrClosed)
	wantState(t, r, "q", State{Open: false, Pending: 1})
}

// ---- broadcast ----

func TestEverySubscriberSeesEveryMessage(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 8, FullBlock)
	tx := mustSender[int](t, r, "b")
	a := mustReceiver[int](t, r, "b")
	b := mustReceiver[int](t, r, "b")
	for i := 1; i <= 3; i++ {
		tx.Send(i)
	}
	for _, rx := range []*Receiver[int]{a, b} {
		got := []int{mustRecv(t, rx), mustRecv(t, rx), mustRecv(t, rx)}
		if !reflect.DeepEqual(got, []int{1, 2, 3}) {
			t.Fatal(got)
		}
	}
}

func TestSubscriptionStartsFromNow(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 8, FullBlock)
	tx := mustSender[int](t, r, "b")
	early := mustReceiver[int](t, r, "b")
	tx.Send(1)
	late := mustReceiver[int](t, r, "b")
	tx.Send(2)
	if got := mustRecv(t, late); got != 2 {
		t.Fatalf("late subscriber got %d, want 2 (no history)", got)
	}
	if got := []int{mustRecv(t, early), mustRecv(t, early)}; !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
}

func TestSendWithoutSubscribersSucceedsAndDiscards(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	tx := mustSender[int](t, r, "b")
	for i := 0; i < 5; i++ { // more than capacity, must not block
		if err := tx.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.TrySend(9); err != nil {
		t.Fatal(err)
	}
	wantState(t, r, "b", State{Open: true, Pending: 0})
	rx := mustReceiver[int](t, r, "b")
	if _, err := rx.TryRecv(); !errors.Is(err, ErrEmpty) {
		t.Fatal(err)
	}
}

func TestBroadcastDropOldestSlowSubscriberLosesOldest(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 2, FullDropOldest)
	tx := mustSender[int](t, r, "b")
	rx := mustReceiver[int](t, r, "b")
	for i := 1; i <= 5; i++ {
		if err := tx.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	if got := []int{mustRecv(t, rx), mustRecv(t, rx)}; !reflect.DeepEqual(got, []int{4, 5}) {
		t.Fatal(got)
	}
	if _, err := rx.TryRecv(); !errors.Is(err, ErrEmpty) {
		t.Fatal(err)
	}
}

func TestBroadcastDropNewestKeepsBufferedMessages(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 2, FullDropNewest)
	tx := mustSender[int](t, r, "b")
	rx := mustReceiver[int](t, r, "b")
	for i := 1; i <= 5; i++ {
		if err := tx.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.TrySend(6); err != nil {
		t.Fatal(err)
	}
	if got := []int{mustRecv(t, rx), mustRecv(t, rx)}; !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
	if _, err := rx.TryRecv(); !errors.Is(err, ErrEmpty) {
		t.Fatal(err)
	}
}

func TestBroadcastBlockTrySendIsAllOrNothing(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	tx := mustSender[int](t, r, "b")
	a := mustReceiver[int](t, r, "b")
	b := mustReceiver[int](t, r, "b")
	if err := tx.TrySend(1); err != nil {
		t.Fatal(err)
	}
	if got := mustRecv(t, a); got != 1 {
		t.Fatal(got)
	}
	// b still holds 1, so 2 reaches neither subscriber.
	wantErr(t, tx.TrySend(2), ErrFull)
	if _, err := a.TryRecv(); !errors.Is(err, ErrEmpty) {
		t.Fatal(err)
	}
	if got := mustRecv(t, b); got != 1 {
		t.Fatal(got)
	}
	if err := tx.TrySend(3); err != nil {
		t.Fatal(err)
	}
	if mustRecv(t, a) != 3 || mustRecv(t, b) != 3 {
		t.Fatal("both subscribers should see 3")
	}
}

func TestBroadcastBlockSendWaitsForSlowestAndLosesNothing(t *testing.T) {
	const n = 100
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 2, FullBlock)
	tx := mustSender[int](t, r, "b")
	fast := mustReceiver[int](t, r, "b")
	slow := mustReceiver[int](t, r, "b")
	go func() {
		for i := 0; i < n; i++ {
			tx.Send(i)
		}
	}()
	fastGot := make(chan []int)
	go func() {
		var got []int
		for i := 0; i < n; i++ {
			got = append(got, mustRecv(t, fast))
		}
		fastGot <- got
	}()
	var slowGot []int
	for i := 0; i < n; i++ {
		time.Sleep(200 * time.Microsecond)
		slowGot = append(slowGot, mustRecv(t, slow))
	}
	want := make([]int, n)
	for i := range want {
		want[i] = i
	}
	if !reflect.DeepEqual(slowGot, want) {
		t.Fatalf("slow subscriber: %v", slowGot)
	}
	if got := <-fastGot; !reflect.DeepEqual(got, want) {
		t.Fatalf("fast subscriber: %v", got)
	}
}

func TestBroadcastBlockSendUnblocksWhenFullSubscriberCloses(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	tx := mustSender[int](t, r, "b")
	stuck := mustReceiver[int](t, r, "b")
	live := mustReceiver[int](t, r, "b")
	tx.Send(1)
	mustRecv(t, live)
	var err error
	done, blocked := blocks(func() { err = tx.Send(2) })
	if !blocked {
		t.Fatal("send should block on the full subscriber")
	}
	stuck.Close()
	finishes(t, done, "blocked send")
	if err != nil {
		t.Fatal(err)
	}
	if got := mustRecv(t, live); got != 2 {
		t.Fatal(got)
	}
}

func TestBroadcastBlockedSendFailsOnShutdownAndSubscribersDrain(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	tx := mustSender[int](t, r, "b")
	rx := mustReceiver[int](t, r, "b")
	tx.Send(1)
	var err error
	done, blocked := blocks(func() { err = tx.Send(2) })
	if !blocked {
		t.Fatal("send should block")
	}
	r.Shutdown("b")
	finishes(t, done, "blocked send")
	// Differs from the Rust crate, where an in-flight send completes after
	// shutdown: here any send that observes shutdown fails.
	wantErr(t, err, ErrClosed)
	if got := mustRecv(t, rx); got != 1 {
		t.Fatal(got)
	}
	_, err = rx.Recv()
	wantErr(t, err, ErrClosed)
}

func TestBroadcastCloneIsIndependentSubscriber(t *testing.T) {
	// Differs from the Rust crate, where clones share one subscription.
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	tx := mustSender[int](t, r, "b")
	a := mustReceiver[int](t, r, "b")
	b := a.Clone()
	tx.Send(7)
	if mustRecv(t, a) != 7 || mustRecv(t, b) != 7 {
		t.Fatal("each clone should get its own copy")
	}
}

func TestBroadcastClosedReceiverNoLongerHoldsMessages(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	tx := mustSender[int](t, r, "b")
	gone := mustReceiver[int](t, r, "b")
	keep := mustReceiver[int](t, r, "b")
	gone.Close()
	gone.Close() // idempotent
	tx.Send(1)
	mustRecv(t, keep)
	if err := tx.TrySend(2); err != nil {
		t.Fatalf("closed subscriber must not apply backpressure: %v", err)
	}
	wantState(t, r, "b", State{Open: true, Pending: 1})
}

func TestBroadcastReceiverAfterCloseReportsClosed(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	tx := mustSender[int](t, r, "b")
	a := mustReceiver[int](t, r, "b")
	keep := mustReceiver[int](t, r, "b")
	tx.Send(1)
	a.Close()
	mustRecv(t, keep) // cleanup drops message 1 from the buffer
	tx.Send(2)
	// Regression: used to index the buffer with a stale cursor and panic.
	if _, err := a.TryRecv(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := a.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestBroadcastCloseWakesBlockedRecv(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 1, FullBlock)
	rx := mustReceiver[int](t, r, "b")
	var err error
	done, blocked := blocks(func() { _, err = rx.Recv() })
	if !blocked {
		t.Fatal("recv should block")
	}
	rx.Close()
	finishes(t, done, "blocked recv")
	wantErr(t, err, ErrClosed)
}

func TestBroadcastTryRecvAfterDestroyReportsClosed(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	tx := mustSender[int](t, r, "b")
	rx := mustReceiver[int](t, r, "b")
	tx.Send(1)
	r.Destroy("b")
	// Regression: used to index the emptied buffer and panic.
	if _, err := rx.TryRecv(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := rx.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	wantErr(t, tx.Send(2), ErrClosed)
}

func TestBroadcastShutdownAdmitsNoNewSubscribers(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	tx := mustSender[int](t, r, "b")
	early := mustReceiver[int](t, r, "b")
	tx.Send(1)
	r.Shutdown("b")
	if _, err := AcquireReceiver[int](r, "b"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	late := early.Clone()
	if _, err := late.TryRecv(); !errors.Is(err, ErrClosed) {
		t.Fatalf("clone after shutdown should be closed: %v", err)
	}
	if got := mustRecv(t, early); got != 1 {
		t.Fatal(got)
	}
	_, err := early.Recv()
	wantErr(t, err, ErrClosed)
}

func TestBroadcastStateReportsWorstSubscriberLag(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 8, FullBlock)
	tx := mustSender[int](t, r, "b")
	fast := mustReceiver[int](t, r, "b")
	slow := mustReceiver[int](t, r, "b")
	tx.Send(1)
	tx.Send(2)
	mustRecv(t, fast)
	wantState(t, r, "b", State{Open: true, Pending: 2})
	mustRecv(t, slow)
	wantState(t, r, "b", State{Open: true, Pending: 1})
	slow.Close()
	wantState(t, r, "b", State{Open: true, Pending: 1})
	mustRecv(t, fast)
	wantState(t, r, "b", State{Open: true, Pending: 0})
}

func TestBroadcastDestroyTakeReturnsBufferedMessages(t *testing.T) {
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	tx := mustSender[int](t, r, "b")
	_ = mustReceiver[int](t, r, "b")
	tx.Send(1)
	tx.Send(2)
	got, err := DestroyTake[int](r, "b")
	if err != nil || !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got, err)
	}
}

func TestBroadcastManySubscribersUnderConcurrency(t *testing.T) {
	const subs, n = 8, 300
	r := NewRegistry()
	mustCreateBroadcast[int](t, r, "b", 4, FullBlock)
	tx := mustSender[int](t, r, "b")
	results := make(chan []int, subs)
	var ready sync.WaitGroup
	for s := 0; s < subs; s++ {
		rx := mustReceiver[int](t, r, "b")
		ready.Add(1)
		go func() {
			ready.Done()
			var got []int
			for {
				v, err := rx.Recv()
				if err != nil {
					break
				}
				got = append(got, v)
			}
			results <- got
		}()
	}
	ready.Wait()
	for i := 0; i < n; i++ {
		if err := tx.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	r.Shutdown("b")
	want := make([]int, n)
	for i := range want {
		want[i] = i
	}
	for s := 0; s < subs; s++ {
		if got := <-results; !reflect.DeepEqual(got, want) {
			t.Fatalf("subscriber saw %d messages (first mismatch around %v)", len(got), firstDiff(got, want))
		}
	}
}

func firstDiff(a, b []int) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// ---- benchmarks ----

func BenchmarkQueueSendRecvPingPong(b *testing.B) {
	r := NewRegistry()
	Create[int](r, "q", 1)
	tx, _ := AcquireSender[int](r, "q")
	rx, _ := AcquireReceiver[int](r, "q")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Send(i)
		rx.Recv()
	}
}

func BenchmarkQueueMPMC(b *testing.B) {
	const producers, consumers = 4, 4
	r := NewRegistry()
	Create[int](r, "q", 1024)
	var wg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		rx, _ := AcquireReceiver[int](r, "q")
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, err := rx.Recv(); err != nil {
					return
				}
			}
		}()
	}
	b.ResetTimer()
	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		tx, _ := AcquireSender[int](r, "q")
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := 0; i < b.N/producers; i++ {
				tx.Send(i)
			}
		}()
	}
	pwg.Wait()
	r.Shutdown("q")
	wg.Wait()
}

func BenchmarkBroadcastFanOut4(b *testing.B) {
	r := NewRegistry()
	CreateBroadcasting[int](r, "b", 1024)
	tx, _ := AcquireSender[int](r, "b")
	var wg sync.WaitGroup
	for s := 0; s < 4; s++ {
		rx, _ := AcquireReceiver[int](r, "b")
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, err := rx.Recv(); err != nil {
					return
				}
			}
		}()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Send(i)
	}
	r.Shutdown("b")
	wg.Wait()
}
