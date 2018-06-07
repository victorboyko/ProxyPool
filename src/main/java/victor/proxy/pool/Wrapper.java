package victor.proxy.pool;

public class Wrapper<T> {
	private T t;
	public Wrapper(T t) {
		this.t = t;
	}
	synchronized T get() {
		return t;
	}
	synchronized void set(T t) {
		this.t = t;
	}
	@Override
	public String toString() {
		return t.toString();
	}
}
