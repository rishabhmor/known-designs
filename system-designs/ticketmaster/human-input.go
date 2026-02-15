package ticketmaster


Functional Requirements ->


	Users should be able to view the current booking of event/artists



	Users should be able to book the event.


	Users should be able to search the event



	Out of Scope->

	Cancelling / Refund of booking




Non Functional Requirements ->


1. Strongly consistent for booking, eventual consistency is fine for search/listing events etc

2. System should be able to list down the available events or search for the available events < 500ms

3. System should be able to tolerate sudden surge in traffic in case lets say a taylor swift event is happening

4. Fault Tolerant / Reliable

Out of Scope

CI/CD
GDPR etc.


	Entities ->

	Ticketmaster ->
	Event, Artist, Ticket, Booking, User

	Hotel Reservation System

	Hotel, HotelInventory { classType - suite, inventory no - 10, classType - normal rooms, invenntory no - 50
	class type - dormitory, inventory no - 100}, Booking, User

	APIs

/api/v1/events -> one lists down events
other lists down hotels

/api/v1/events/{event_id} -> specific event details

/api/v1/events/{event_id}/inventory ->

/api/v1/events/{event_id}/book -> {tickets -> t1, t2, t3, t4 ...} -
	Response -> reserved success, with reservation id

book will do a reservation- > reserve inventory with a booking id,
create a transcation on the payment system and return with the redirection link.

	Consumer goes through and makes the payment, payment system calls our webhook
Consumer keeps on polling for the status via /api/v1/booking/{booking_id}/ -> Processing, Booked
if booked share the booking details.

Database Entities ->

Hotel -> Id, Details , Location etc

HotelInventory
HotelId, Date -> Available Counter


bookingId, hotelId -> Reserved

hotelId, bookingId

	GSI on hotelId, bookingId to get all the bookings for a hotel

