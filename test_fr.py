from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()
flights = fr_api.get_flights()
print(f"Total flights: {len(flights)}")
if flights:
    f = flights[0]
    print(f.callsign, getattr(f, 'origin_airport_iata', 'N/A'), getattr(f, 'destination_airport_iata', 'N/A'))
