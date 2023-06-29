DROP TABLE result;

CREATE TABLE result (
	id int NOT NULL,
	response text NULL
);

INSERT INTO result SELECT 1,count(passenger_name) over (partition by book_ref) AS booking_count from bookings.tickets
order by booking_count desc
limit 1;
INSERT INTO result select 2,count(book_ref) from (
SELECT book_ref, avg (booking_count) as booking_count from(
SELECT book_ref, (count(passenger_name) over (partition by book_ref)) AS booking_count from bookings.tickets
)a
group by book_ref
having avg(booking_count)>(SELECT avg (booking_count) as booking_count from(
SELECT book_ref, (count(passenger_name) over (partition by book_ref)) AS booking_count from bookings.tickets
)a))a;
INSERT INTO result (select 3,count (*) from (
select passenger_name_array, count(*) from(
select  book_ref, STRING_AGG(passenger_name,'|' ORDER BY passenger_name) "passenger_name_array" from bookings.tickets where 
book_ref in (
select book_ref from (
SELECT book_ref, count(*) AS booking_count from bookings.tickets
GROUP by book_ref
order by count(*) desc
)a where booking_count=5)
group by book_ref)a
group by passenger_name_array
having count(*)>1)a);
INSERT INTO result (select 4, concat(book_ref,'|',array_agg(passenger_id||'|'||passenger_name||'|'||contact_data)) "data" from bookings.tickets where 
book_ref in (
select book_ref from (
SELECT book_ref, count(*) AS booking_count from bookings.tickets
group by book_ref
order by count(*) desc
)a where booking_count=3)
group by book_ref
order by data);
INSERT INTO result (select 5, max(flight_id_count) from(
select count(tf.flight_id) as flight_id_count, t.book_ref 
from bookings.ticket_flights tf 
left join bookings.tickets t on tf.ticket_no = t.ticket_no
group by t.book_ref)a);
INSERT INTO result (select 6, max(ticket_no_count) from(
select count(tf.ticket_no) as ticket_no_count, t.book_ref, t.passenger_name, t.passenger_id
from bookings.ticket_flights tf 
left join bookings.tickets t on tf.ticket_no = t.ticket_no
group by t.book_ref, t.passenger_name,  t.passenger_id)a);
INSERT INTO result (select 7, max(ticket_no_count) from(
select count(tf.ticket_no) as ticket_no_count, t.passenger_id, t.passenger_name
from bookings.ticket_flights tf 
left join bookings.tickets t on tf.ticket_no = t.ticket_no
group by t.passenger_id, t.passenger_name)a);
INSERT INTO result (select 8, concat(passenger_id,'|',passenger_name,'|',contact_data,'|', amount) from (
select t.passenger_id,t.passenger_name,t.contact_data, (tf.amount), min(tf.amount) over (partition by true) as min_amount
from bookings.ticket_flights tf 
left join bookings.tickets t on tf.ticket_no = t.ticket_no
order by (tf.amount)) a
where amount<=min_amount
);
INSERT INTO result (select 9, concat(passenger_id,'|',passenger_name,'|',contact_data,'|',all_time) from(
select passenger_id, passenger_name, contact_data, (sum(all_time) over (partition by passenger_id, passenger_name)) "all_time"   from(
select fl.flight_id, t.ticket_no, t.passenger_id, t.passenger_name, t.contact_data,
case when (actual_arrival is not null and actual_departure is not null) then actual_arrival-actual_departure else scheduled_arrival-scheduled_departure end as all_time
from bookings.flights fl 
left join bookings.ticket_flights tf on fl.flight_id = tf.flight_id
left join bookings.tickets t on tf.ticket_no = t.ticket_no
where status in ('Arrived')
and t.passenger_name is not null
)a
order by all_time desc
limit 1)a);
INSERT INTO result (select 10, city from(
select city, count(airport_name) as airport_count from bookings.airports
group by city
having count(airport_name)>1)a
order by city);
INSERT INTO result (select 11, departure_city from (
select departure_city, count_connect, min(count_connect) over (partition by null) as min_citiyes from(
select departure_city, count(arrival_city) as count_connect from(
select distinct(departure_city), (a.city) as arrival_city from (
select fl.departure_airport, fl.arrival_airport,
case when fl.departure_airport=a.airport_code then a.city else null end "departure_city"
from bookings.flights fl left join bookings.airports a on fl.departure_airport=a.airport_code)b 
left join bookings.airports a on arrival_airport=a.airport_code)a
group by departure_city
order by count_connect)a)a
where count_connect=min_citiyes
order by departure_city);
INSERT INTO result (select 12, cities from(
select concat(city_1, '|', city_2) cities from (
select city_1, city_2 from (
select a.city as city_1, b.city as city_2
from bookings.airports a full join bookings.airports b on true where a.city!=b.city
EXCEPT
select distinct(departure_city) as city_1, (a.city) as city_2 from (
select fl.departure_airport, fl.arrival_airport,
case when fl.departure_airport=a.airport_code then a.city else null end "departure_city"
from bookings.flights fl left join bookings.airports a on fl.departure_airport=a.airport_code)b 
left join bookings.airports a on arrival_airport=a.airport_code)a
EXCEPT
select  distinct(a.city) as city_1, departure_city as city_2 from (
select fl.departure_airport, fl.arrival_airport,
case when fl.departure_airport=a.airport_code then a.city else null end "departure_city"
from bookings.flights fl left join bookings.airports a on fl.departure_airport=a.airport_code)b 
left join bookings.airports a on arrival_airport=a.airport_code)a)a
order by cities);
INSERT INTO result (select 13, arrival_city from (
select distinct(arrival_city) from(
select distinct(departure_city), (a.city) as arrival_city from (
select fl.departure_airport, fl.arrival_airport,
case when fl.departure_airport=a.airport_code then a.city else null end "departure_city"
from bookings.flights fl left join bookings.airports a on fl.departure_airport=a.airport_code)b 
left join bookings.airports a on arrival_airport=a.airport_code)a
where departure_city='Москва' or arrival_city='Москва'
except 
select distinct(arrival_city) from(
select distinct(departure_city), (a.city) as arrival_city from (
select fl.departure_airport, fl.arrival_airport,
case when fl.departure_airport=a.airport_code then a.city else null end "departure_city"
from bookings.flights fl left join bookings.airports a on fl.departure_airport=a.airport_code)b 
left join bookings.airports a on arrival_airport=a.airport_code)a
where departure_city!='Москва' and arrival_city!='Москва') a
where arrival_city != 'Москва'
order by arrival_city);
INSERT INTO result (select 14, model from (
select a.model, count(flight_id) from bookings.flights f
left join bookings.aircrafts a on f.aircraft_code=a.aircraft_code
group by a.model
order by count(flight_id) desc 
limit 1)a);
INSERT INTO result (select 15, a.model from bookings.boarding_passes bp
left join bookings.flights f on bp.flight_id=f.flight_id
left join bookings.aircrafts a on f.aircraft_code=a.aircraft_code 
group by a.model
order by count (bp.boarding_no) desc
limit 1);
INSERT INTO result (select 16, EXTRACT(EPOCH from (sum(time)))/60 as time from(
select case when (scheduled_arrival-scheduled_departure) > (actual_arrival-actual_departure) then (scheduled_arrival-scheduled_departure) - (actual_arrival-actual_departure) else 
(actual_arrival-actual_departure) - (scheduled_arrival-scheduled_departure) end as time from bookings.flights f
where status='Arrived')a);
INSERT INTO result (select 17, city from (
select DISTINCT(a.city) from bookings.flights fl left join bookings.airports a on fl.arrival_airport=a.airport_code
where departure_airport = 'LED'
and status = 'Arrived'
and actual_departure >= '2016-09-13'
and actual_departure < '2016-09-14')a);
INSERT INTO result (select 18, flight_id from(
select flight_id, amount, max(amount) over(partition by true) as maximum from (
select flight_id, sum(amount) as amount from bookings.ticket_flights tf 
group by flight_id
order by sum(amount) desc)a)a
where amount=maximum);
INSERT INTO result (select 19, flight_date from(
select min(count_flight_id) over (partition by true) as minimum, count_flight_id, flight_date from(
select count(flight_id) as count_flight_id , date_trunc('day', actual_departure) as flight_date from bookings.flights f
where status='Arrived'
group by date_trunc('day', actual_departure)
order by count(flight_id))a)a 
where count_flight_id=minimum);
INSERT INTO result (select 20, AVG(flight_count) from (
select date_trunc('day', actual_departure) as flight_day, count(flight_id) as flight_count
from bookings.flights fl left join bookings.airports a on fl.departure_airport = a.airport_code
where a.city='Москва'
and status = 'Arrived'
and actual_departure >= '2016-09-01'
and actual_departure <= '2016-09-30'
group by date_trunc('day', actual_departure))a);
INSERT INTO result (select 21, city from ( 
select fl.arrival_airport, fl.departure_airport, fl.actual_arrival-fl.actual_departure as time, a.city
from bookings.flights fl left join bookings.airports a on fl.arrival_airport = a.airport_code
where fl.status = 'Arrived')a
group by city
having EXTRACT(EPOCH from avg(time))/60/24>3
order by EXTRACT(EPOCH from avg(time))/60/24 DESC
limit 5);
