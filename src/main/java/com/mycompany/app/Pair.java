package com.mycompany.app;

public class Pair<G,T> {

	private G first;
	private T second;
	
	public Pair(G first, T second) {
		super();
		this.first = first;
		this.second = second;
	}

	public G getFirst() {
		return first;
	}

	public void setFirst(G first) {
		this.first = first;
	}

	public T getSecond() {
		return second;
	}

	public void setSecond(T second) {
		this.second = second;
	}
}
