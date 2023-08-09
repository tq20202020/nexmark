package com.github.nexmark.flink.model;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * An event in the auction system, either a (new) {@link Person}, a (new) {@link Auction}, or a
 * {@link Bid}.
 */
public class Event {

	public @Nullable Person newPerson;
	public @Nullable Auction newAuction;
	public @Nullable Bid bid;
	public Type type;

	/** The type of object stored in this event. * */
	public enum Type {
		PERSON(0),
		AUCTION(1),
		BID(2);

		public final int value;

		Type(int value) {
			this.value = value;
		}
	}

	public Event(Person newPerson) {
		this.newPerson = newPerson;
		newAuction = null;
		bid = null;
		type = Type.PERSON;
	}

	public Event(Auction newAuction) {
		newPerson = null;
		this.newAuction = newAuction;
		bid = null;
		type = Type.AUCTION;
	}

	public Event(Bid bid) {
		newPerson = null;
		newAuction = null;
		this.bid = bid;
		type = Type.BID;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Event event = (Event) o;
		return Objects.equals(newPerson, event.newPerson)
			&& Objects.equals(newAuction, event.newAuction)
			&& Objects.equals(bid, event.bid);
	}

	@Override
	public int hashCode() {
		return Objects.hash(newPerson, newAuction, bid);
	}

	@Override
	public String toString() {
		if (newPerson != null) {
			return newPerson.toString();
		} else if (newAuction != null) {
			return newAuction.toString();
		} else if (bid != null) {
			return bid.toString();
		} else {
			throw new RuntimeException("invalid event");
		}
	}
}
